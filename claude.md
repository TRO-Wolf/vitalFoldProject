# VitalFold Data Pipeline Project

## Project Overview
Data engineering portfolio project building a medallion-architecture data pipeline on top of the VitalFold simulation engine. The engine (at `/home/john/CodeRepos/vitalFoldEngine/`) generates synthetic cardiac clinic data into Aurora DSQL (13 tables, ~500K rows) and DynamoDB (2 tables).

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
                                         dbt via Athena (aggregate)
                                                        │
                                                S3 Gold (Iceberg)
                                                        │
                                        Athena / Superset (dashboard)

Orchestration: Airflow 3.0 (local Docker Compose)
IaC: Terraform
```

## VitalFold Engine API (upstream data source)
The engine is not a passive data source — it has an API-driven 3-phase population workflow:
1. **Static Populate** (`POST /populate/static`) — seeds reference data (patients, providers, clinics, insurance). Run once; 409 if already exists.
2. **Dynamic Populate** (`POST /populate/dynamic`) — seeds date-dependent data (appointments, visits, vitals) for a date range (max 90 days, no overlap).
3. **DynamoDB Sync** (`POST /simulate/date-range`) — reads Aurora visits for a date range and writes to both DynamoDB tables.

All endpoints require JWT auth (`POST /api/v1/auth/admin-login`), return 202, and are polled via `GET /simulate/status` until `running == false`.
Verification via `GET /simulate/db-counts` returns row counts from both Aurora and DynamoDB.
Full API details in [docs/airflow-integration.md](docs/airflow-integration.md).

## Tech Stack
| Component | Choice |
|-----------|--------|
| Orchestration | Airflow 3.0 (local Docker Compose, LocalExecutor) |
| Spark | AWS Glue 5.1 (serverless, Spark 3.5.2, Iceberg 1.10/V3) |
| Transformations | dbt-core + dbt-athena (merged into dbt-adapters monorepo) |
| Storage | S3 + Apache Iceberg via Glue Data Catalog |
| IaC | Terraform |
| Dashboard | Apache Superset (local Docker, PyAthena driver) |
| Data Quality | dbt tests + custom PySpark assertions |
| CI/CD | GitHub Actions |

## Source Data (VitalFold Engine)
- **Aurora DSQL tables**: insurance_company (7), insurance_plan (21), clinic (10), provider (50), patient (50K), emergency_contact (50K), patient_demographics (50K), patient_insurance (~50K), clinic_schedule (~500), appointment (100K), medical_record (100K), patient_visit (100K), patient_vitals (100K)
- **DynamoDB tables** (2 tables, both use composite sort key `clinic_id#patient_visit_id`):
  - `patient_visit` (PK: patient_id, SK: clinic_id#patient_visit_id) — checkin/checkout times, provider_seen_time, ekg_usage, estimated_copay, creation_time, record_expiration_epoch
  - `patient_vitals` (PK: patient_id, SK: clinic_id#patient_visit_id) — visit_id, height, weight, blood_pressure (VARCHAR "SYS/DIA"), heart_rate (number), temperature, oxygen (note: field is "oxygen" not "oxygen_saturation"), creation_time, record_expiration_epoch
- **NOTE**: dynamo.json is authoritative (validated against Rust source code in generators/appointment.rs). dynamo.md is outdated and should be regenerated.
- Schemas are in `docs/schemas/` (copied from engine repo)

## Medallion Layers

### Bronze — `s3://vitalfold-lakehouse/bronze/`
Raw ingestion. Aurora via JDBC (full snapshot for reference tables, incremental for large tables). DynamoDB via export. Metadata columns: `_source_system`, `_ingested_at`, `_batch_id`.

### Silver — `s3://vitalfold-lakehouse/silver/` (strictly Iceberg tables)
Cleaned/conformed Iceberg tables written by Glue Spark using `MERGE INTO` / `INSERT OVERWRITE`. Registered in Glue Data Catalog under `vitalfold_silver` database. Dedup DynamoDB vs Aurora (DynamoDB is source of truth for visit-time data).

**Dimensions:**
- `dim_patient` (SCD2 with valid_from/valid_to)
- `dim_provider`
- `dim_clinic`
- `dim_insurance` (plan + company joined)
- `dim_date` (standard calendar dimension for join-based date analytics)

**Facts:**
- `fact_appointment` (includes derived `no_show_flag` via LEFT JOIN to visits)
- `fact_visit` (includes `appointment_duration_minutes` derived from checkin/checkout, denormalized `insurance_plan_id`)
- `fact_vitals` (blood_pressure parsed into `systolic_bp` / `diastolic_bp` integers; DynamoDB field `oxygen` mapped to `oxygen_saturation`; `visit_id` used to join back to fact_visit)
- `fact_medical_record`

**Bridges:**
- `bridge_patient_insurance` (patient ↔ insurance plan many-to-many)
- `bridge_provider_clinic` (provider ↔ clinic relationships from clinic_schedule)

### Gold — `s3://vitalfold-lakehouse/gold/` (strictly Iceberg tables)
Business aggregates written as Iceberg tables. Registered in Glue Data Catalog under `vitalfold_gold` database.

**NOTE**: dbt-athena Iceberg write support must be validated in Phase 0. If dbt-athena cannot materialize as Iceberg via Athena CTAS, Gold layer falls back to Glue Spark for Iceberg writes with dbt used only for model SQL logic.

Tables: clinic_daily_metrics, clinic_monthly_summary, patient_risk_profile, patient_cohort_analysis, insurance_plan_metrics, provider_workload.

## Repo Structure
```
vitalFoldProject/
├── claude.md
├── Makefile
├── pyproject.toml
├── .gitignore
├── docker-compose.yml              # Root-level — single `docker compose up` to run everything
├── .env.example                    # Environment variables for Docker Compose
├── docker/
│   └── airflow/Dockerfile          # Custom Airflow image (providers, dbt, boto3)
├── airflow/dags/
│   ├── vitalfold_populate.py       # DAG: API-driven data population (static → dynamic → DynamoDB sync → verify)
│   ├── vitalfold_daily_sync.py     # DAG: Daily DynamoDB sync for a single day
│   ├── bronze_ingestion.py         # DAG: Aurora + DynamoDB → Bronze Iceberg (triggered after populate)
│   ├── silver_transform.py         # DAG: Bronze → Silver Iceberg (Glue jobs)
│   ├── gold_aggregate.py           # DAG: Silver → Gold Iceberg (dbt via Athena)
│   └── full_pipeline.py            # Master DAG: populate → bronze → silver → gold → quality
├── spark/
│   ├── jobs/bronze/    (ingest_aurora.py, ingest_dynamodb.py)
│   ├── jobs/silver/    (clean_patients.py, clean_appointments.py, clean_visits.py, clean_vitals.py, clean_reference.py)
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
│   └── modules/        (s3/, glue/, iam/, networking/)
├── scripts/            (bootstrap.sh, upload_spark_jobs.sh, seed_connections.sh)
├── docs/
│   ├── architecture.md         # Built incrementally: Phase 1 (overview), Phase 3 (dedup strategy), Phase 4 (orchestration), Phase 5 (final)
│   ├── data_dictionary.md      # Built incrementally: Phase 2 (Bronze), Phase 3 (Silver), Phase 4 (Gold)
│   ├── airflow-integration.md  # VitalFold Engine API reference + example DAGs (already written)
│   ├── setup_guide.md          # Environment setup: AWS credentials, Terraform state backend, Engine connection, Airflow variables
│   ├── runbook.md              # Ops guide: reset/repopulate, re-run failed phases, extend date ranges, troubleshooting
│   ├── validation_log.md       # Phase 0 findings (DynamoDB schema, JDBC test, dbt-athena Iceberg verdict)
│   ├── cost_log.md             # Actual AWS costs tracked per phase (Glue DPU, Athena scans, S3)
│   └── schemas/                (health_clinic_schema.sql, dynamo.md, dynamo.json — validated in Phase 0)
└── .github/workflows/  (lint_and_test.yml, deploy_glue_jobs.yml)
```

## Implementation Steps

### Phase 0 — Validation (before building anything) 🔶
- [x] 0.1 ~~Resolve DynamoDB schema ambiguity~~ — RESOLVED: 2 separate tables (patient_visit + patient_vitals), composite sort key `clinic_id#patient_visit_id`, vitals field is `oxygen` not `oxygen_saturation`. dynamo.json is authoritative (validated against Rust source in generators/appointment.rs). dynamo.md is outdated.
- [ ] 0.2 Test JDBC connectivity from Glue to Aurora DSQL (driver compatibility, connection string)
- [ ] 0.3 Test dbt-athena Iceberg materialization: can `CREATE TABLE AS SELECT ... format='ICEBERG'` work via dbt? If not, plan Gold layer via Glue Spark instead
- [ ] 0.4 Confirm dbt-athena package name (standalone was archived Sept 2025, now in dbt-adapters monorepo)
- [ ] 0.5 Document findings: update docs/schemas/ with validated DynamoDB schema, record JDBC connection string, record dbt-athena Iceberg verdict in docs/validation_log.md

### Phase 1 — Foundation ⬜
- [ ] 1.1 Create repo structure (all directories, .gitignore, pyproject.toml, Makefile)
- [ ] 1.2 Copy schemas from engine repo into docs/schemas/ (update with Phase 0 validated schemas)
- [ ] 1.3 Terraform: S3 buckets (vitalfold-lakehouse, vitalfold-glue-assets)
- [ ] 1.4 Terraform: Glue Data Catalog databases (bronze, silver, gold)
- [ ] 1.5 Terraform: IAM roles (Glue execution role)
- [ ] 1.6 docker-compose.yml at project root: Airflow 3.0 (webserver, scheduler, triggerer, postgres) + .env.example
- [ ] 1.7 docker/airflow/Dockerfile: custom image (providers-amazon, dbt-core, dbt-athena, boto3)
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
- [ ] 2.5 Spark job: ingest_aurora.py (JDBC read all 13 tables → Bronze Iceberg)
- [ ] 2.6 Spark job: ingest_dynamodb.py (export/scan → Bronze Iceberg; must parse composite sort key `clinic_id#patient_visit_id` into separate `clinic_id` and `patient_visit_id` columns)
- [ ] 2.7 Terraform: Glue jobs for bronze ingestion
- [ ] 2.8 Airflow DAG: bronze_ingestion.py (GlueJobOperator triggers, runs after populate completes)
- [ ] 2.9 Verify Bronze tables queryable via Athena
- [ ] 2.10 Document: add docstrings to each Spark job (purpose, source tables, target tables, incremental strategy, partition scheme)
- [ ] 2.11 Document: add inline comments to all DAGs (populate flow, task dependencies, retry logic, polling strategy, connection references)
- [ ] 2.12 Document: begin docs/data_dictionary.md — Bronze layer section (table name, source, columns, types, metadata columns, partitioning, update frequency)
- [ ] 2.13 Document: record actual Glue DPU usage and job duration for cost tracking in docs/cost_log.md
- [ ] 2.14 Document: write docs/runbook.md — initial sections: how to reset and repopulate data (reset endpoints), how to re-run a failed Bronze ingestion, how to extend the date range with a new dynamic populate call

### Phase 3 — Silver Layer ⬜
- [ ] 3.1 Spark job: build_dim_date.py (standard calendar dimension)
- [ ] 3.2 Spark job: clean_reference.py (dim_clinic, dim_provider, dim_insurance, bridge_provider_clinic)
- [ ] 3.3 Spark job: clean_patients.py (SCD2 dim_patient, demographics, emergency contacts)
- [ ] 3.4 Spark job: clean_appointments.py (fact_appointment with derived no_show_flag via LEFT JOIN to visits)
- [ ] 3.5 Spark job: clean_visits.py (fact_visit with appointment_duration_minutes, denormalized insurance_plan_id; dedup Aurora vs DynamoDB)
- [ ] 3.6 Spark job: clean_vitals.py (fact_vitals with blood_pressure parsed into systolic_bp/diastolic_bp integers; dedup Aurora vs DynamoDB)
- [ ] 3.7 Spark job: clean_medical_records.py (fact_medical_record)
- [ ] 3.8 Data quality assertions in Spark jobs (vital sign ranges, referential integrity, dedup verification)
- [ ] 3.9 Terraform: Glue jobs for silver transforms
- [ ] 3.10 Airflow DAG: silver_transform.py
- [ ] 3.11 Verify Silver tables in Athena (dedup correct, SCD2 working, BP parsed)
- [ ] 3.12 Document: add docstrings to each Silver Spark job (transformation logic, dedup strategy, SCD2 approach, quality checks performed)
- [ ] 3.13 Document: inline comments in quality.py explaining each assertion (valid ranges, thresholds, failure behavior)
- [ ] 3.14 Document: extend docs/data_dictionary.md — Silver layer section (each dim/fact/bridge table with columns, types, derivation logic, source Bronze tables, SCD type, grain)
- [ ] 3.15 Document: add Silver layer dedup strategy explanation to docs/architecture.md (why DynamoDB is source of truth for visit data, reconciliation logic)
- [ ] 3.16 Document: update docs/cost_log.md with Silver Glue job actual DPU and duration

### Phase 4 — Gold Layer + dbt ⬜
- [ ] 4.1 Initialize dbt project with dbt-athena adapter (from dbt-adapters monorepo)
- [ ] 4.2 Staging models (thin views over Silver Iceberg tables)
- [ ] 4.3 Mart: clinic_performance.sql (daily metrics, monthly summary)
- [ ] 4.4 Mart: patient_risk_scores.sql (rolling vitals, risk flags)
- [ ] 4.5 Mart: insurance_utilization.sql (plan metrics, coverage gaps)
- [ ] 4.6 Mart: provider_workload.sql
- [ ] 4.7 dbt tests (not_null, unique, accepted_values, custom range tests)
- [ ] 4.8 Airflow DAG: gold_aggregate.py (BashOperator runs dbt)
- [ ] 4.9 Wire full_pipeline.py master DAG (populate → bronze → silver → gold → quality)
- [ ] 4.10 Document: add dbt model descriptions in _staging.yml and _marts.yml (column descriptions, business definitions, test explanations)
- [ ] 4.11 Document: add header comments to each .sql model (business question answered, source Silver tables, grain, update frequency)
- [ ] 4.12 Document: extend docs/data_dictionary.md — Gold layer section (each model with columns, business meaning, derivation from Silver, refresh cadence)
- [ ] 4.13 Document: generate dbt docs site (`dbt docs generate`) and include instructions for viewing in README
- [ ] 4.14 Document: add full_pipeline.py DAG inline comments (end-to-end flow, Data Assets usage, dependency chain, failure/retry behavior)
- [ ] 4.15 Document: update docs/architecture.md with pipeline orchestration section (DAG structure, task groups, scheduling strategy, data-aware triggers)

### Phase 5 — Dashboard + Polish ⬜
- [ ] 5.1 Add Superset to Docker Compose (port 8088, PyAthena driver)
- [ ] 5.2 Build clinic performance dashboard
- [ ] 5.3 Build patient risk dashboard
- [ ] 5.4 Build insurance utilization dashboard
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
- [ ] 5.14 Document: add Superset setup instructions to README (how to connect to Athena, import dashboards)

## Key Design Decisions
- **Spark for Bronze/Silver, dbt for Gold**: Heavy data movement uses Spark; SQL aggregation uses dbt. Shows both skills.
- **Iceberg (v1.10, Format V3)**: De facto standard in 2026 (78.6% exclusive adoption). Broadest engine support (Spark, Athena, Trino, Flink). Silver and Gold layers are strictly Iceberg tables. All tables registered in Glue Data Catalog.
- **Glue 5.1 over EMR Serverless**: Zero infra, native Iceberg V3 support, Glue Catalog integrated. EMR Serverless is cheaper per-hour but more setup.
- **Standard S3 + Iceberg over S3 Tables**: S3 Tables is 36% more expensive and less portable. Standard Iceberg-on-S3 shows deeper understanding of internals.
- **Airflow 3.0 Data Assets**: Use Airflow 3.0's Data Assets for data-aware scheduling (not just task dependencies). Demonstrates modern 3.0 features.
- **No Redshift**: $2.88/hr minimum, doesn't add skills beyond Athena+dbt. Stretch goal.
- **No streaming**: Data is batch-generated. DynamoDB Streams is a documented stretch goal. Batch still critical in 2026 for cost-effective transforms and backfills.
- **No Great Expectations**: dbt tests + Spark checks cover needs without framework overhead. Could add Elementary Data for dbt-native observability as stretch goal.
- **Schemas copied into repo**: Self-contained for GitHub reviewers.
- **DuckDB for local testing**: Use for Spark job unit tests instead of full PySpark (handles 50GB on a laptop).

## Cost Estimate
~$50-60/month (realistic estimate, excluding Aurora DSQL which already exists)
- Glue jobs: 6-7 jobs × 2 DPU × ~0.5 hr/run × 20 runs/month = ~$40-55
- Athena: ~10 GB scanned/month (dbt + ad-hoc) = ~$0.50-3
- S3 storage: ~15 GB (Bronze + Silver + Gold) = ~$0.35
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
- dbt-athena-community repo archived Sept 2025 → use dbt-athena from dbt-adapters monorepo
- Iceberg write via Athena CTAS is unverified → added Phase 0 validation step
- DynamoDB schema docs are ambiguous → added Phase 0 validation step
- JDBC to Aurora DSQL untested → added Phase 0 validation step
- Cost was underestimated (was $10-20, now $50-60) → corrected
- Missing dim_date, no_show_flag, BP parsing → added to Silver layer

**Portfolio differentiators to emphasize:**
- Dual-source deduplication (Aurora + DynamoDB) — not a toy pipeline
- SCD Type 2 on patient dimension — shows data modeling depth
- Strictly Iceberg Silver/Gold — demonstrates format expertise
- Airflow 3.0 Data Assets — shows you're on the cutting edge
- Realistic healthcare data (50K patients, 100K appointments) — meaningful scale
