# VitalFold - Project Task Plan

> **Status note (2026-05):** This is the original phased build plan, moved out of
> [CLAUDE.md](../../../CLAUDE.md) so that file stays focused on architecture and design
> context. Several phases below predate the architecture pivot from dbt-redshift +
> Redshift Spectrum to **dbt-spark + Iceberg-everywhere** - treat any Redshift / Spectrum /
> Superset-on-Redshift step as historical intent, not current direction. For the accurate,
> current public-readiness punch list see [docs/portfolio-gaps.md](../../portfolio-gaps.md).
>
> **Convention:** open work lives in `docs/task/pending/`; as phases close out, move the
> finished items (or the whole file) to `docs/task/completed/`.

---

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
