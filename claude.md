# VitalFold Data Pipeline Project

## Project Overview
Data engineering portfolio project building a medallion-architecture data pipeline on top of the VitalFold simulation engine. The engine is public at https://github.com/TRO-Wolf/VitalFoldSimulator (local mirror: `/home/john/CodeRepos/vitalFoldEngine/`). It generates synthetic cardiac clinic data into Aurora DSQL (15 tables in `vital_fold` schema, ~750K+ rows) and DynamoDB (2 tables). The engine ships with CPT/RVU Medicare billing data and patient satisfaction surveys, enabling real healthcare finance analytics in the Gold layer.

## Architecture
```
VitalFold Engine API
  ├── POST /populate/static    ──> Aurora DSQL (reference data)
  ├── POST /populate/dynamic   ──> Aurora DSQL (appointments, visits, vitals)
  └── POST /simulate/date-range ──> DynamoDB (patient_visit, patient_vitals)
         │
         v
Aurora DSQL (13 tables) ──JDBC──> Glue Spark ──> S3 Bronze (Iceberg)
DynamoDB (2 tables)     ──Export──> Glue Spark ──> S3 Bronze (Iceberg)
                                                        │
                                          Glue Spark (clean, dedup, conform)
                                                        │
                                               S3 Silver (Iceberg)
                                                        │
                                          ┌─────────────┘
                                          v
                              Redshift Serverless (Spectrum)
                              reads Silver Iceberg via external schema
                                          │
                                dbt-redshift (aggregate)
                                          │
                              Redshift-native Gold tables
                                          │
                              Superset ──> Redshift (JDBC)

Orchestration: Airflow 3.0 (local Docker Compose)
IaC: Terraform
```

## VitalFold Engine API (upstream data source)
Public repo: https://github.com/TRO-Wolf/VitalFoldSimulator — Rust/Actix-web, 22 endpoints, runs on port **8787**.

The engine is not a passive data source — it has an API-driven 3-phase population workflow:
1. **Static Populate** (`POST /populate/static`) — seeds reference data (patients, providers, clinics, insurance, CPT codes). Run once; 409 if already exists.
2. **Dynamic Populate** (`POST /populate/dynamic`) — seeds date-dependent data (appointments, visits, vitals, appointment_cpt billing lines, surveys) for a date range (max 90 days, no overlap).
3. **DynamoDB Sync** (`POST /simulate/date-range`) — reads Aurora visits for a date range and writes to both DynamoDB tables.

All endpoints require JWT auth (`POST /api/v1/auth/admin-login`), return 202, and are polled via `GET /simulate/status` until `running == false`.
- `GET /populate/dates` — list dates already populated (use for incremental watermark in Airflow)
- `GET /simulate/db-counts` — row counts from both Aurora and DynamoDB (verification)
- `POST /simulate/reset` — clear all data (destructive; Phase 0 testing only)
- `POST /admin/init-db` — recreate schema from migrations

Full API details in [docs/airflow-integration.md](docs/airflow-integration.md).

## Tech Stack
| Component | Choice |
|-----------|--------|
| Orchestration | Airflow 3.0 (local Docker Compose, LocalExecutor) |
| Spark | AWS Glue 5.1 (serverless, Spark 3.5.2, Iceberg 1.10/V3) |
| Transformations | dbt-core + dbt-redshift |
| Storage (Bronze/Silver) | S3 + Apache Iceberg via Glue Data Catalog |
| Storage (Gold) | Redshift Serverless (native tables) |
| Query/Serving | Redshift Serverless (Spectrum for Iceberg reads, native for Gold) |
| IaC | Terraform |
| Dashboard | Apache Superset (local Docker, Redshift JDBC) |
| Data Quality | dbt tests + custom PySpark assertions |
| CI/CD | GitHub Actions |

## Source Data (VitalFold Engine)
**Aurora DSQL schema**: all tables live in the `vital_fold` schema (NOT `public`). JDBC queries must qualify table names as `vital_fold.<table>`.

**Aurora DSQL tables (15 in vital_fold schema)**:
- Reference/dims: `insurance_company` (7), `insurance_plan` (21), `clinic` (10, BIGINT id), `provider` (50, BIGINT id), `cpt_code` (seeded Medicare RVU reference, BIGINT id)
- Patient core: `patient` (50K), `emergency_contact` (50K), `patient_demographics` (50K), `patient_insurance` (~50K)
- Scheduling: `clinic_schedule` (~500)
- Clinical facts: `appointment` (~162K per 90-day run — 50 providers × 36 slots × 90 days; has `status` column: ~90% completed, ~9% cancelled, ~1% no_show), `medical_record` (~145K, only for completed), `patient_visit` (~145K, only for completed), `patient_vitals` (~145K, PK = patient_visit_id, height/weight/oxygen_saturation are NULLABLE)
- **Billing (NEW)**: `appointment_cpt` (~162K+ line items with work_rvu_snapshot, pe_rvu_snapshot, mp_rvu_snapshot, total_rvu_snapshot, conversion_factor, expected_amount — real Medicare RVU economics with CY2024 conversion factor $32.7442)
- **Satisfaction (NEW)**: `survey` (~30% of visits get one — gene_prissy_score, experience_score, feedback_comments TEXT)

**DynamoDB tables** (2 tables, both use composite sort key `clinic_id#patient_visit_id`):
- `patient_visit` (PK: patient_id, SK: clinic_id#patient_visit_id) — checkin/checkout times, provider_seen_time, ekg_usage, estimated_copay, creation_time, record_expiration_epoch
- `patient_vitals` (PK: patient_id, SK: clinic_id#patient_visit_id) — visit_id, height, weight, blood_pressure (VARCHAR "SYS/DIA"), heart_rate, temperature, **oxygen** (DynamoDB uses short name; Aurora uses `oxygen_saturation`), creation_time, record_expiration_epoch

**Schema notes**:
- `patient_vitals` primary key is `patient_visit_id` (1:1 with visits, not a separate vitals_id)
- `appointment.status` is VARCHAR(20) DEFAULT 'completed' — only completed appointments generate downstream visits/vitals/billing/surveys
- `appointment_cpt` is a fact-grain line-item table — one row per CPT code per appointment. This is the revenue backbone. Only exists for completed appointments.
- `survey` only exists for ~30% of **completed** visits — Gold models must use LEFT JOIN
- `feedback_comments` is free-text TEXT column — useful for downstream NLP stretch goal
- Schemas in `docs/schemas/` should be refreshed from https://github.com/TRO-Wolf/VitalFoldSimulator/blob/main/vital-fold-engine/migrations/init.sql

**Simulated Data Quality Issues** (intentionally injected by the engine for pipeline stress-testing):

| Category | Issue | Rate | Affected Table(s) | Pipeline Impact |
|----------|-------|------|--------------------|-----------------|
| **Appointment Status** | No-call no-shows | ~1% | `appointment` (status='no_show') | No downstream visit/vitals/billing records exist; use status column directly for no-show metrics |
| **Appointment Status** | Cancellations | ~9% | `appointment` (status='cancelled') | Same — no downstream records; derive cancellation rate per clinic/provider |
| **Missing Vitals** | Nullable height, weight, oxygen_saturation | ~3% of visits | `patient_vitals` | Must handle NULLs in aggregations; COALESCE or exclude from averages |
| **Vital Outliers** | Clinically extreme values | ~2% of visits | `patient_vitals` | Fever 100.5-104°F, hypertensive crisis 180-220/100-130, bradycardia 30-45 bpm, tachycardia 130-180 bpm, hypoxemia 70-94% O₂. Flag/quarantine, don't silently include in averages |
| **Late Arrivals** | Checkin time after appointment time | ~2% of visits | `patient_visit` | Derive `is_late_arrival` flag and `late_minutes` (checkin_time - appointment_datetime). Affects wait time calculations |
| **Duplicate SSNs** | Shared SSN across patients | ~2% of patients | `patient_demographics` | No UNIQUE constraint; SCD2 dedup must handle — flag but don't reject (real-world SSN collisions happen) |
| **Duplicate Emails** | Shared email across patients | ~3% of patients | `patient` | No UNIQUE constraint; flag in quality report |
| **Duplicate Policy Numbers** | Shared policy_number | ~1% of records | `patient_insurance` | No UNIQUE constraint; flag in quality report |
| **Clinical Contradictions** | Diagnosis ↔ vitals mismatch | Random | `medical_record` + `patient_vitals` | "Bradycardia" diagnosis with HR 120 bpm is intentional; don't try to "fix" — flag as contradiction in quality report |
| **Stale Age** | Age computed once, never updated | 100% | `patient_demographics` | Derive age from `date_of_birth` in Silver, ignore source `age` column |
| **Middle Name Sparsity** | Now ~40% populated | ~40% | `patient` | Handle in SCD2 — middle_name going from NULL to populated is a legitimate change event |

## Medallion Layers

### Bronze — `s3://vitalfold-lakehouse/bronze/`
Production-grade config-driven ingestion via YAML table manifest (`spark/config/table_manifest.yml`).

**Table Tiering:**
- **Tier 1 — Static Reference** (6 tables, <1K rows): `insurance_company`, `insurance_plan`, `provider`, `clinic`, `cpt_code`, `clinic_schedule` — FULL OVERWRITE every run, sequential, unpartitioned, < 1 min. No watermark columns exist.
- **Tier 2 — Patient Core** (4 tables, ~200K rows): `patient`, `emergency_contact`, `patient_demographics`, `patient_insurance` — FULL initial, then incremental by `registration_date`/`coverage_start_date`. Tables without timestamps (emergency_contact, patient_demographics) do full snapshot every run (50K rows = pennies in Glue). Patient partitioned by `year(registration_date)`.
- **Tier 3 — High-Volume Facts** (6 tables, ~850K+ rows): `appointment`, `medical_record`, `patient_visit`, `patient_vitals`, `appointment_cpt`, `survey` — FULL initial with JDBC parallel reads (`numPartitions=12`), then incremental by `creation_time`/`appointment_datetime`. All partitioned by `days(<timestamp>)` in Iceberg.
- **DynamoDB** (2 tables): DynamoDB Export to S3 → Spark → Bronze Iceberg. Always full export + `INSERT OVERWRITE`. Parse composite sort key `clinic_id#patient_visit_id` into separate columns.

**Load Ordering:** Tier 1 (sequential, <1 min) → Tier 2 (sequential, ~2-3 min) → Tier 3 + DynamoDB (PARALLEL, ~5-10 min). Catches issues on cheap small tables before burning DPU on big ones.

**Metadata Columns:** `_source_system`, `_source_table`, `_ingested_at`, `_batch_id` (= Airflow dag_run_id), `_load_type` ("initial" or "incremental")

**Watermark Tracking:** Stored in Airflow Variables (`bronze_watermark_<table>`). Set after successful load, read at next DAG run, passed as Glue job arg. Fallback: if missing/corrupt → full snapshot.

**Row Count Reconciliation:** Source COUNT vs Bronze COUNT after every load. Fail if mismatch > 0.1%.

**Idempotency:** Tier 1 = INSERT OVERWRITE. Tier 2/3 initial = INSERT OVERWRITE by partition. Tier 2/3 incremental = APPEND (dedup handled in Silver). DynamoDB = INSERT OVERWRITE. Re-runs never create duplicates.

### Silver — `s3://vitalfold-lakehouse/silver/` (strictly Iceberg tables)
Cleaned/conformed Iceberg tables written by Glue Spark using `MERGE INTO` / `INSERT OVERWRITE`. Registered in Glue Data Catalog under `vitalfold_silver` database. Dedup DynamoDB vs Aurora (DynamoDB is source of truth for visit-time data).

**Dimensions:**
- `dim_patient` (SCD2 with valid_from/valid_to; derive `age` from `date_of_birth` — ignore source `age` column which is stale. Middle name changes from NULL to populated trigger SCD2 version. Flag duplicate SSNs and emails with `_has_duplicate_ssn` / `_has_duplicate_email` boolean columns — don't reject, real-world collisions happen)
- `dim_provider`
- `dim_clinic`
- `dim_insurance` (plan + company joined; flag duplicate policy numbers with `_has_duplicate_policy`)
- `dim_date` (standard calendar dimension for join-based date analytics)
- `dim_cpt_code` (Medicare CPT reference with work_rvu, pe_rvu, mp_rvu, category, description)

**Facts:**
- `fact_appointment` (includes `status` column directly from source: 'completed'/'no_show'/'cancelled'. No longer needs LEFT JOIN to visits — status is authoritative. Derive `is_no_show`, `is_cancelled` booleans for easy filtering. Only ~90% are completed.)
- `fact_visit` (includes `appointment_duration_minutes` derived from checkin/checkout, denormalized `insurance_plan_id`; derive `is_late_arrival` flag and `late_minutes` when checkin_time > appointment_datetime; dedup Aurora vs DynamoDB)
- `fact_vitals` (blood_pressure parsed into `systolic_bp` / `diastolic_bp` integers; height/weight/oxygen_saturation are NULLABLE — preserve NULLs, don't impute. Add `_is_outlier` flag for extreme values using clinical thresholds: temp >104 or <94°F, systolic >220 or <70, HR >180 or <30, O₂ <70%. Do NOT exclude outliers — flag them so Gold can choose to include/exclude.)
- `fact_medical_record` (note: diagnosis ↔ vitals contradictions are intentional and should NOT be "fixed" — flag as `_has_clinical_contradiction` if diagnosis text implies opposite of vitals)
- `fact_billing_line` (grain: one row per appointment_cpt line item; preserves RVU snapshots, modifier_1/modifier_2, units, conversion_factor, expected_amount; only exists for completed appointments; join key to fact_appointment + dim_cpt_code)
- `fact_survey` (grain: one row per survey; gene_prissy_score, experience_score, feedback_comments; LEFT JOIN to fact_visit since ~30% of completed visits have surveys)

**Bridges:**
- `bridge_patient_insurance` (patient ↔ insurance plan many-to-many; flag `_has_duplicate_policy`)
- `bridge_provider_clinic` (provider ↔ clinic relationships from clinic_schedule)

### Gold — Redshift Serverless `vitalfold_gold` schema (native tables)
Business aggregates materialized as Redshift-native tables by dbt-redshift. Redshift reads Silver Iceberg tables via **Spectrum external schema** (`vitalfold_silver_ext`) pointing to Glue Data Catalog `vitalfold_silver`. Gold tables live inside Redshift for fast dashboard queries.

Tables:
- `clinic_daily_metrics` — appointments/day, **no-show rate** (from status column), **cancellation rate**, avg wait time, late arrival rate, provider utilization
- `clinic_monthly_summary` — MoM trends, capacity analysis, no-show/cancellation trends over time
- `patient_risk_profile` — rolling avg vitals (**excluding outliers** via `_is_outlier` flag), visit frequency, cardiac risk flags, null vitals frequency as a data completeness signal
- `patient_cohort_analysis` — outcomes by age (**derived from DOB**, not stale source age)/insurance/clinic cohort
- `insurance_plan_metrics` — covered patients, visit density, claim patterns per plan
- `provider_workload` — daily patient count, avg appointment duration, **late arrival impact** (avg late_minutes per provider)
- `provider_rvu_productivity` — wRVU per provider per day, total collections, expected revenue (healthcare finance gold-standard metric)
- `revenue_by_payer` — expected_amount aggregated by insurance_company × clinic × month; shows payer-mix profitability
- `clinic_financial_performance` — total billed, expected collections, avg RVU per visit, procedure mix per clinic
- `patient_satisfaction_trends` — avg survey scores by clinic/provider/month, NPS-style bucketing, trend vs prior period
- **`data_quality_report`** (NEW) — per-run summary: outlier count/rate, null vitals count/rate, duplicate SSN/email/policy counts, late arrival count/rate, clinical contradiction count, appointment status breakdown. Demonstrates pipeline observability.

## Repo Structure
```
vitalFoldProject/
├── claude.md
├── Makefile
├── pyproject.toml
├── .gitignore
├── docker-compose.yml              # Root-level — single `docker compose up` to run everything
├── .env.example                    # AWS config template (AWS_ACCOUNT_ID, AWS_REGION, bucket names)
├── docker/
│   └── airflow/
│       ├── Dockerfile                  # Custom Airflow image (Spark, Iceberg JARs, providers, dbt, boto3)
│       ├── spark-defaults.conf.template  # Spark config with ${VAR} placeholders (envsubst at startup)
│       └── entrypoint.sh              # Runs envsubst to resolve spark-defaults.conf from .env vars
├── airflow/dags/
│   ├── vitalfold_populate.py       # DAG: API-driven data population (static → dynamic → DynamoDB sync → verify)
│   ├── vitalfold_daily_sync.py     # DAG: Daily DynamoDB sync for a single day
│   ├── bronze_ingestion.py         # DAG: Aurora + DynamoDB → Bronze Iceberg (triggered after populate)
│   ├── silver_transform.py         # DAG: Bronze → Silver Iceberg (Glue jobs)
│   ├── gold_aggregate.py           # DAG: Silver → Gold Redshift (dbt-redshift via Spectrum)
│   └── full_pipeline.py            # Master DAG: populate → bronze → silver → gold → quality
├── spark/
│   ├── config/         (table_manifest.yml — config-driven ingestion: table name, tier, PK, watermark, partition, JDBC splits)
│   ├── jobs/bronze/    (ingest_aurora.py, ingest_dynamodb.py)
│   ├── jobs/silver/    (clean_patients.py, clean_appointments.py, clean_visits.py, clean_vitals.py, clean_reference.py, clean_billing.py, clean_surveys.py)
│   ├── jobs/gold/
│   ├── lib/            (utils.py, quality.py)
│   └── tests/
├── dbt/
│   ├── models/staging/ (stg_patients.sql, stg_appointments.sql, _staging.yml)
│   ├── models/marts/   (clinic_performance.sql, patient_risk_scores.sql, insurance_utilization.sql, _marts.yml)
│   ├── macros/
│   └── tests/
├── terraform/
│   ├── main.tf, variables.tf, outputs.tf
│   └── modules/        (s3/, glue/, redshift/, iam/, networking/)
├── scripts/            (bootstrap.sh, upload_spark_jobs.sh, seed_connections.sh)
├── docs/
│   ├── architecture.md         # Built incrementally: Phase 1 (overview), Phase 3 (dedup strategy), Phase 4 (orchestration), Phase 5 (final)
│   ├── data_dictionary.md      # Built incrementally: Phase 2 (Bronze), Phase 3 (Silver), Phase 4 (Gold)
│   ├── airflow-integration.md  # VitalFold Engine API reference + example DAGs (already written)
│   ├── setup_guide.md          # Environment setup: AWS credentials, Terraform state backend, Engine connection, Airflow variables
│   ├── runbook.md              # Ops guide: reset/repopulate, re-run failed phases, extend date ranges, troubleshooting
│   ├── validation_log.md       # Phase 0 findings (DynamoDB schema, JDBC test, Redshift Spectrum Iceberg verdict)
│   ├── cost_log.md             # Actual AWS costs tracked per phase (Glue DPU, Redshift RPU-hours, S3)
│   └── schemas/                (health_clinic_schema.sql, dynamo.md, dynamo.json — validated in Phase 0)
└── .github/workflows/  (lint_and_test.yml, deploy_glue_jobs.yml)
```

## Implementation Steps

### Phase 0 — Validation (before building anything) 🔶
- [x] 0.1 ~~Resolve DynamoDB schema ambiguity~~ — RESOLVED: 2 separate tables (patient_visit + patient_vitals), composite sort key `clinic_id#patient_visit_id`, DynamoDB vitals field is `oxygen` (Aurora column is `oxygen_saturation`).
- [ ] 0.1a Refresh `docs/schemas/` from public repo: pull latest `migrations/init.sql` from https://github.com/TRO-Wolf/VitalFoldSimulator — note schema is `vital_fold.*` (NOT public), and includes new `cpt_code`, `appointment_cpt`, `survey` tables.
- [ ] 0.2 Test JDBC connectivity from Glue to Aurora DSQL (driver compatibility, connection string, `vital_fold` schema qualifier)
- [ ] 0.3 Test Redshift Serverless Spectrum → Silver Iceberg: create external schema pointing to Glue Catalog `vitalfold_silver`, verify `SELECT` against Iceberg tables works
- [ ] 0.4 Confirm dbt-redshift adapter version and connectivity (pip install dbt-redshift, verify connection profile against Redshift Serverless workgroup)
- [ ] 0.5 Document findings: update docs/schemas/ with latest init.sql, record JDBC connection string + schema qualifier, record Redshift Spectrum Iceberg verdict in docs/validation_log.md

### Phase 1 — Foundation ⬜
- [ ] 1.1 Create repo structure (all directories, .gitignore, pyproject.toml, Makefile)
- [ ] 1.2 Copy schemas from engine repo into docs/schemas/ (update with Phase 0 validated schemas)
- [ ] 1.3 Terraform: S3 buckets (vitalfold-lakehouse, vitalfold-glue-assets)
- [ ] 1.4 Terraform: Glue Data Catalog databases (bronze, silver)
- [ ] 1.5 Terraform: IAM roles (Glue execution role, Redshift Spectrum role)
- [ ] 1.5a Terraform: Redshift Serverless (namespace + workgroup, 8 RPU base)
- [ ] 1.5b Terraform: Redshift external schema (`vitalfold_silver_ext` → Glue Catalog) + internal schema (`vitalfold_gold`)
- [ ] 1.6 docker-compose.yml at project root: Airflow 3.0 (webserver, scheduler, triggerer, postgres) + .env.example
- [ ] 1.7 docker/airflow/Dockerfile: custom image (providers-amazon, dbt-core, dbt-redshift, boto3)
- [ ] 1.8 Verify Airflow UI at localhost:8080 with hello-world DAG
- [ ] 1.9 Document: write initial README.md (project overview, architecture diagram, tech stack, prerequisites, `docker compose up` instructions)
- [ ] 1.10 Document: write docs/architecture.md (high-level data flow, medallion layer descriptions, technology choices with rationale, cost estimate)
- [ ] 1.11 Document: write docs/setup_guide.md (AWS credentials config, Terraform state backend setup, VitalFold Engine connection, Airflow variables, .env.example walkthrough)
- [ ] 1.12 Document: add inline comments to Terraform modules explaining each resource and its role
- [ ] 1.13 Document: add inline comments to docker-compose.yml explaining each service, port, and volume mount

### Phase 2 — Data Population + Bronze Layer ⬜
- [ ] 2.1 Airflow DAG: vitalfold_populate.py (authenticate → static populate → dynamic populate → DynamoDB sync → verify counts). Use example from docs/airflow-integration.md as starting point.
- [ ] 2.2 Airflow DAG: vitalfold_daily_sync.py (authenticate → check dates → sync single day → verify). Use daily sync example from docs/airflow-integration.md.
- [ ] 2.3 Set up Airflow Variables (vitalfold_base_url, admin credentials) via seed_connections.sh
- [ ] 2.4 Test population DAGs end-to-end against running VitalFold Engine
- [ ] 2.5 Write `spark/config/table_manifest.yml` — define all 15 Aurora tables with tier, PK, watermark_column, partition_by, jdbc_partitions
- [ ] 2.6 Spark job: ingest_aurora.py — config-driven: reads table_manifest.yml, loops by tier (Tier 1 sequential OVERWRITE → Tier 2 sequential → Tier 3 parallel with JDBC partitioning). Appends 5 metadata columns. Row count reconciliation per table.
- [ ] 2.7 Spark job: ingest_dynamodb.py (export to S3 → Spark → Bronze Iceberg; parse composite sort key `clinic_id#patient_visit_id` into separate columns; INSERT OVERWRITE)
- [ ] 2.8 Terraform: Glue jobs for bronze ingestion
- [ ] 2.9 Airflow DAG: bronze_ingestion.py (GlueJobOperator triggers with tier-ordered TaskGroups: reference → patient_core → facts_parallel → dynamodb; watermark read/write via Airflow Variables)
- [ ] 2.10 Verify Bronze tables queryable via Athena — row counts match source for all 17 tables (15 Aurora + 2 DynamoDB)
- [ ] 2.10 Document: add docstrings to each Spark job (purpose, source tables, target tables, incremental strategy, partition scheme)
- [ ] 2.11 Document: add inline comments to all DAGs (populate flow, task dependencies, retry logic, polling strategy, connection references)
- [ ] 2.12 Document: begin docs/data_dictionary.md — Bronze layer section (table name, source, columns, types, metadata columns, partitioning, update frequency)
- [ ] 2.13 Document: record actual Glue DPU usage and job duration for cost tracking in docs/cost_log.md
- [ ] 2.14 Document: write docs/runbook.md — initial sections: how to reset and repopulate data (reset endpoints), how to re-run a failed Bronze ingestion, how to extend the date range with a new dynamic populate call

### Phase 3 — Silver Layer ⬜
- [ ] 3.1 Spark job: build_dim_date.py (standard calendar dimension)
- [ ] 3.2 Spark job: clean_reference.py (dim_clinic, dim_provider, dim_insurance, dim_cpt_code, bridge_provider_clinic)
- [ ] 3.3 Spark job: clean_patients.py (SCD2 dim_patient, demographics, emergency contacts)
- [ ] 3.4 Spark job: clean_appointments.py (fact_appointment: map `status` column to `is_no_show`/`is_cancelled` booleans; validate status distribution ~90/9/1; verify no downstream records exist for no_show/cancelled appointments)
- [ ] 3.5 Spark job: clean_visits.py (fact_visit: derive `appointment_duration_minutes`, `is_late_arrival`, `late_minutes`; denormalize insurance_plan_id; dedup Aurora vs DynamoDB)
- [ ] 3.6 Spark job: clean_vitals.py (fact_vitals: parse BP into systolic/diastolic; preserve NULLs in height/weight/oxygen_saturation; add `_is_outlier` flag using clinical thresholds; dedup Aurora vs DynamoDB)
- [ ] 3.7 Spark job: clean_medical_records.py (fact_medical_record: add `_has_clinical_contradiction` flag when diagnosis text contradicts vitals — e.g. "Bradycardia" with HR >100)
- [ ] 3.7a Spark job: clean_billing.py (fact_billing_line from appointment_cpt; only completed appointments; RVU snapshots, expected_amount, modifiers)
- [ ] 3.7b Spark job: clean_surveys.py (fact_survey; only completed visits; experience_score, gene_prissy_score, feedback_comments)
- [ ] 3.8 Data quality assertions in Spark jobs:
  - Vital sign outlier rate ~2% (not 0% — outliers are real, not bugs)
  - Null vitals rate ~3% for height/weight/oxygen
  - Appointment status distribution within tolerance (~90/9/1 ± 2%)
  - No visits/vitals/billing/surveys for no_show or cancelled appointments
  - Duplicate SSN rate ~2%, duplicate email rate ~3%, duplicate policy ~1% (flag, don't reject)
  - Late arrival rate ~2%
  - RVU non-negativity, survey scores within bounds
  - Referential integrity: every visit has a completed appointment
- [ ] 3.9 Terraform: Glue jobs for silver transforms
- [ ] 3.10 Airflow DAG: silver_transform.py
- [ ] 3.11 Verify Silver tables in Athena (dedup correct, SCD2 working, BP parsed)
- [ ] 3.11a Verify Silver Iceberg tables accessible from Redshift via Spectrum external schema
- [ ] 3.12 Document: add docstrings to each Silver Spark job (transformation logic, dedup strategy, SCD2 approach, quality checks performed)
- [ ] 3.13 Document: inline comments in quality.py explaining each assertion (valid ranges, thresholds, failure behavior)
- [ ] 3.14 Document: extend docs/data_dictionary.md — Silver layer section (each dim/fact/bridge table with columns, types, derivation logic, source Bronze tables, SCD type, grain)
- [ ] 3.15 Document: add Silver layer dedup strategy explanation to docs/architecture.md (why DynamoDB is source of truth for visit data, reconciliation logic)
- [ ] 3.16 Document: update docs/cost_log.md with Silver Glue job actual DPU and duration

### Phase 4 — Gold Layer + dbt-redshift ⬜
- [ ] 4.1 Initialize dbt project with dbt-redshift adapter (profiles.yml targeting Redshift Serverless workgroup)
- [ ] 4.2 Staging models (views over Silver Iceberg tables via Spectrum external schema `vitalfold_silver_ext`)
- [ ] 4.3 Mart: clinic_performance.sql (daily metrics, monthly summary)
- [ ] 4.4 Mart: patient_risk_scores.sql (rolling vitals, risk flags)
- [ ] 4.5 Mart: insurance_utilization.sql (plan metrics, coverage gaps)
- [ ] 4.6 Mart: provider_workload.sql
- [ ] 4.6a Mart: provider_rvu_productivity.sql (NEW — wRVU/day, expected collections, procedure mix by provider)
- [ ] 4.6b Mart: revenue_by_payer.sql (NEW — expected_amount × insurance_company × clinic × month)
- [ ] 4.6c Mart: clinic_financial_performance.sql (NEW — total billed, expected collections, avg RVU per visit, denial proxy)
- [ ] 4.6d Mart: patient_satisfaction_trends.sql (NEW — avg scores by clinic/provider/month with trend vs prior period)
- [ ] 4.7 dbt tests (not_null, unique, accepted_values, custom range tests, RVU sanity checks)
- [ ] 4.8 Airflow DAG: gold_aggregate.py (BashOperator runs dbt)
- [ ] 4.9 Wire full_pipeline.py master DAG (populate → bronze → silver → gold → quality)
- [ ] 4.10 Document: add dbt model descriptions in _staging.yml and _marts.yml (column descriptions, business definitions, test explanations)
- [ ] 4.11 Document: add header comments to each .sql model (business question answered, source Silver tables, grain, update frequency)
- [ ] 4.12 Document: extend docs/data_dictionary.md — Gold layer section (each model with columns, business meaning, derivation from Silver, refresh cadence)
- [ ] 4.13 Document: generate dbt docs site (`dbt docs generate`) and include instructions for viewing in README
- [ ] 4.14 Document: add full_pipeline.py DAG inline comments (end-to-end flow, Data Assets usage, dependency chain, failure/retry behavior)
- [ ] 4.15 Document: update docs/architecture.md with pipeline orchestration section (DAG structure, task groups, scheduling strategy, data-aware triggers)

### Phase 5 — Dashboard + Polish ⬜
- [ ] 5.1 Add Superset to Docker Compose (port 8088, sqlalchemy-redshift + redshift_connector driver)
- [ ] 5.2 Build clinic performance dashboard
- [ ] 5.3 Build patient risk dashboard
- [ ] 5.4 Build insurance utilization dashboard
- [ ] 5.4a Build provider RVU productivity + revenue by payer dashboard (NEW — healthcare finance focus)
- [ ] 5.4b Build patient satisfaction trends dashboard (NEW — survey-driven)
- [ ] 5.5 GitHub Actions: lint_and_test.yml (ruff, sqlfluff, pytest, dbt compile)
- [ ] 5.6 GitHub Actions: deploy_glue_jobs.yml (upload to S3, terraform apply)
- [ ] 5.7 Document: add inline comments to GitHub Actions workflows (trigger conditions, job steps, secrets required)
- [ ] 5.8 Document: finalize docs/architecture.md — add dashboard layer description, CI/CD pipeline explanation, environment strategy
- [ ] 5.8a Document: finalize docs/runbook.md — add Silver/Gold troubleshooting, Superset connection issues, full end-to-end recovery procedure
- [ ] 5.9 Document: finalize docs/data_dictionary.md — review all layers for completeness, add glossary of healthcare terms used
- [ ] 5.10 Document: finalize docs/cost_log.md — final cost breakdown with actual measurements from all phases
- [ ] 5.11 Document: write Design Decisions section in README (why each tech choice, what was considered and rejected, trade-offs made)
- [ ] 5.12 Document: write Lessons Learned section in README (bugs encountered, surprises, what you'd do differently)
- [ ] 5.13 Document: finalize README.md — full setup instructions (prerequisites, clone, Docker up, Terraform apply, trigger pipeline, view dashboards), architecture diagram, link to dbt docs, link to cost log
- [ ] 5.14 Document: add Superset setup instructions to README (how to connect to Redshift, import dashboards)

## Key Design Decisions
- **Spark for Bronze/Silver, dbt-redshift for Gold**: Heavy data movement uses Spark; SQL aggregation uses dbt. Redshift Spectrum bridges the two — reads Iceberg, writes native tables. Demonstrates proficiency in Spark, dbt, Iceberg, AND Redshift.
- **Iceberg for Bronze/Silver, Redshift-native for Gold**: Silver is strictly Iceberg (open format, portable). Gold is Redshift-native for fast dashboard queries. Spectrum connects them seamlessly. Shows understanding of when to use open formats vs warehouse-native storage.
- **Redshift Serverless over Provisioned**: Scales to zero when idle — only charges per RPU-hour ($0.375/RPU-hr). At this project's scale, ~$5-15/month for dbt runs + ad-hoc queries. No cluster management.
- **Iceberg (v1.10, Format V3)**: De facto standard in 2026 (78.6% exclusive adoption). Broadest engine support (Spark, Athena, Trino, Flink, Redshift Spectrum).
- **Glue 5.1 over EMR Serverless**: Zero infra, native Iceberg V3 support, Glue Catalog integrated. EMR Serverless is cheaper per-hour but more setup.
- **Standard S3 + Iceberg over S3 Tables**: S3 Tables is 36% more expensive and less portable. Standard Iceberg-on-S3 shows deeper understanding of internals.
- **Airflow 3.0 Data Assets**: Use Airflow 3.0's Data Assets for data-aware scheduling (not just task dependencies). Demonstrates modern 3.0 features.
- **No streaming**: Data is batch-generated. DynamoDB Streams is a documented stretch goal. Batch still critical in 2026 for cost-effective transforms and backfills.
- **No Great Expectations**: dbt tests + Spark checks cover needs without framework overhead. Could add Elementary Data for dbt-native observability as stretch goal.
- **Schemas copied into repo**: Self-contained for GitHub reviewers.
- **DuckDB for local testing**: Use for Spark job unit tests instead of full PySpark (handles 50GB on a laptop).

## Cost Estimate
~$55-75/month (realistic estimate, excluding Aurora DSQL which already exists)
- Glue jobs: 6-7 jobs × 2 DPU × ~0.5 hr/run × 20 runs/month = ~$40-55
- Redshift Serverless: ~$5-15/month (RPU-hours for dbt runs + ad-hoc queries, scales to zero when idle)
- S3 storage: ~15 GB (Bronze + Silver) = ~$0.35
- S3 requests + Iceberg overhead = ~$1
- Local Docker (Airflow, Superset) = $0
- **Tip**: Set AWS cost alerts, measure actual Glue DPU usage in Phase 2, adjust

## 2026 Architecture Review Notes
*Reviewed March 2026 against current data engineering landscape*

**Validated choices:**
- Iceberg is the clear winner (v1.10.1, Format V3, 78.6% adoption)
- Medallion still dominant (adapting per-domain in data mesh orgs, but standard for portfolio projects)
- Airflow 3.0 released April 2025, now at 3.1.8 — our choice is current
- dbt remains the transformation standard (dbt Fusion in beta but too new)
- Glue 5.1 has native Iceberg V3 + deletion vectors — better than our original Glue 4.0 plan

**Risks identified and mitigated:**
- dbt-athena-community repo archived Sept 2025 → switched to dbt-redshift (more robust, Redshift Serverless scales to zero)
- Redshift Spectrum reading Iceberg → needs Phase 0 validation
- DynamoDB schema docs are ambiguous → added Phase 0 validation step
- JDBC to Aurora DSQL untested → added Phase 0 validation step
- Cost was underestimated (was $10-20, now $55-75) → corrected with Redshift Serverless added
- Missing dim_date, no_show_flag, BP parsing → added to Silver layer

**Portfolio differentiators to emphasize:**
- Dual-source deduplication (Aurora + DynamoDB) — not a toy pipeline
- SCD Type 2 on patient dimension — shows data modeling depth
- Iceberg Silver + Redshift Gold via Spectrum — shows open format ↔ warehouse integration
- Redshift Serverless + dbt-redshift — highly demanded skill on DE job postings
- **Real Medicare RVU economics** — wRVU/day, expected collections, payer-mix analysis using CY2024 Medicare conversion factor ($32.7442). Healthcare-specific finance domain knowledge, not generic aggregates.
- **Patient satisfaction analytics** — LEFT JOIN survey data (~30% coverage) with trend analysis; demonstrates sparse-data handling
- **Dirty data handling** — pipeline handles 11 distinct simulated data quality issues (no-shows, cancellations, null vitals, outliers, late arrivals, duplicate SSNs/emails/policies, clinical contradictions, stale age, sparse middle names). Flag-don't-reject strategy with `_is_outlier`, `_has_duplicate_ssn`, `_has_clinical_contradiction` columns. This is what separates a production pipeline from a demo.
- **Data quality observability** — Gold-layer `data_quality_report` table tracks quality metrics per pipeline run; demonstrates pipeline monitoring
- Airflow 3.0 Data Assets — shows you're on the cutting edge
- **Upstream data source is public** — https://github.com/TRO-Wolf/VitalFoldSimulator reviewers can clone and reproduce
- Realistic healthcare data (50K patients, ~145K completed appointments + ~16K no-shows/cancellations, 162K+ billing lines, ~50K surveys per 90-day run) — meaningful scale with realistic data quality challenges
