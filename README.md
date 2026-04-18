# VitalFold Data Pipeline

> **In plain English:** A data pipeline for a fictional network of 10 cardiac clinics. It pulls raw patient visits, vital signs, and billing records from two different databases, cleans and combines them, then produces executive dashboards showing revenue, provider productivity, patient outcomes, and satisfaction trends. Same medallion + Iceberg + Redshift stack used by Fortune 500 hospital systems.

## Business Outcomes

The pipeline answers real questions a cardiac clinic operator would ask:

| Theme | Questions Answered |
|-------|--------------------|
| **Operations** | How many appointments per clinic per day? What's the no-show rate? Are providers overloaded? |
| **Clinical** | Which patients show deteriorating cardiac indicators? How do outcomes differ by age and insurance? |
| **Finance** | What's each provider's daily wRVU output and expected collections? Which insurance companies drive the most revenue? Which clinics perform best financially? |
| **Satisfaction** | How are patient satisfaction scores trending per clinic and provider? |

All metrics are built on real Medicare RVU economics (CY2024 conversion factor $32.7442), not made-up numbers.

## Dashboards

_Dashboard screenshots will be added after Phase 5 (Superset build-out). Planned views:_

- **Operations dashboard** — clinic-level daily appointments, no-show heatmap, provider utilization
- **Finance dashboard** — provider wRVU productivity, revenue by payer, clinic financial performance
- **Clinical dashboard** — patient risk profile, cohort outcomes, vitals trends
- **Satisfaction dashboard** — survey score trends by clinic and provider

## What This Project Demonstrates

- **Medallion architecture** (Bronze / Silver / Gold) on Apache Iceberg tables in S3
- **Dual-source ingestion** from Aurora DSQL (15 relational tables) and DynamoDB (2 NoSQL tables) with cross-source deduplication
- **Dimensional modeling** with SCD Type 2 slowly changing dimensions, derived metrics, and bridge tables
- **Orchestration** with Apache Airflow 3.0, including API-driven data population and Glue job management
- **Transformations** split by purpose: PySpark (Glue) for heavy Bronze/Silver ETL, dbt-redshift for Gold-layer SQL analytics
- **Open-format ↔ warehouse integration** via Redshift Spectrum reading Silver Iceberg tables through an external schema
- **Infrastructure as Code** via Terraform (S3, Glue, Redshift Serverless, IAM, Data Catalog)
- **Data quality** enforced at every layer through dbt tests and PySpark assertions
- **Dirty-data handling** for 11 simulated real-world data quality issues (no-shows, cancellations, null vitals, clinical outliers, late arrivals, duplicate SSNs/emails/policies, clinical contradictions, stale age, sparse middle names) using a flag-don't-reject strategy

## Architecture

```
                        VitalFold Engine API
                 (Rust, Actix-web — public repo)
                               │
          ┌────────────────────┼────────────────────┐
          v                    v                    v
   POST /populate/static  POST /populate/dynamic  POST /simulate/date-range
          │                    │                    │
          v                    v                    v
   Aurora DSQL             Aurora DSQL           DynamoDB
   (reference data,        (appointments,        (patient_visit,
    cpt_code seed)          visits, vitals,       patient_vitals)
                            billing, surveys)
          │                    │                    │
          └────────┬───────────┘                    │
                   v                                v
            Glue Spark (JDBC)                 Glue Spark (Export)
                   │                                │
                   └──────────┬─────────────────────┘
                              v
                    ┌─────────────────┐
                    │  S3 Bronze      │  Raw Iceberg tables
                    │  (15 Aurora +   │  + metadata columns
                    │   2 DynamoDB)   │
                    └────────┬────────┘
                             v
                    ┌─────────────────┐
                    │  Glue Spark     │  Clean, dedup, conform
                    │  (Silver ETL)   │  SCD2, BP parsing, RVU mapping
                    └────────┬────────┘
                             v
                    ┌─────────────────┐
                    │  S3 Silver      │  Iceberg dims, facts, bridges
                    │  (14 tables)    │  Glue Data Catalog
                    └────────┬────────┘
                             v
                    ┌─────────────────────────┐
                    │  Redshift Serverless    │  Spectrum external schema
                    │  (reads Silver Iceberg) │  reads Iceberg directly
                    └────────┬────────────────┘
                             v
                    ┌─────────────────┐
                    │  dbt-redshift   │  SQL aggregations
                    │  (Gold ETL)     │  10 business models
                    └────────┬────────┘
                             v
                    ┌─────────────────┐
                    │  Redshift-native│  Fast dashboard tables
                    │  Gold schema    │  vitalfold_gold.*
                    └────────┬────────┘
                             v
                    ┌─────────────────┐
                    │  Superset       │  Dashboards via Redshift JDBC
                    └─────────────────┘

   Orchestration: Airflow 3.0 (local Docker Compose)
   Infrastructure: Terraform
```

## Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| Data Source | [VitalFold Engine](https://github.com/TRO-Wolf/VitalFoldSimulator) (Rust/Actix-web) | Generates 750K+ synthetic cardiac clinic records including Medicare RVU billing and satisfaction surveys |
| Primary Storage | Aurora DSQL (PostgreSQL-compatible) | 15 relational tables — patients, providers, appointments, vitals, billing, surveys |
| Event Storage | DynamoDB | 2 tables — real-time visit and vitals capture |
| Orchestration | Apache Airflow 3.0 | DAG-based pipeline orchestration with Data Assets |
| Batch Processing | AWS Glue 5.1 (Spark 3.5.2) | Bronze/Silver ETL with native Iceberg V3 support |
| Table Format (Bronze/Silver) | Apache Iceberg (v1.10, Format V3) | ACID transactions, schema evolution, time travel |
| Catalog | AWS Glue Data Catalog | Iceberg metadata catalog for Bronze and Silver |
| Warehouse (Gold) | Amazon Redshift Serverless | Native Gold tables; reads Silver Iceberg via Spectrum; scales to zero |
| Transformations | dbt-core + dbt-redshift | Gold-layer SQL models, testing, documentation |
| Dashboards | Apache Superset | Interactive analytics dashboards via Redshift JDBC |
| Infrastructure | Terraform | S3 buckets, Glue jobs, Redshift Serverless, IAM roles, Data Catalog |
| CI/CD | GitHub Actions | Linting, testing, Glue job deployment |

## Data Pipeline

### Source Data

The VitalFold Engine generates realistic cardiac clinic data for a network of 10 clinics across the southeastern United States:

| Data | Volume | Source |
|------|--------|--------|
| Patients | 50,000 | Aurora DSQL |
| Providers | 50 (cardiac specialists) | Aurora DSQL |
| Clinics | 10 (SE US) | Aurora DSQL |
| Insurance Plans | 21 (7 companies x 3 plans) | Aurora DSQL |
| Appointments | ~162,000 per 90-day run (~90% completed, ~9% cancelled, ~1% no-show) | Aurora DSQL |
| Medical Records | ~145,000 (completed only) | Aurora DSQL |
| Patient Visits | ~145,000 (completed only) | Aurora DSQL + DynamoDB |
| Patient Vitals | ~145,000 (completed only) | Aurora DSQL + DynamoDB |
| CPT Billing Lines | ~145,000+ (Medicare RVU-priced) | Aurora DSQL |
| Patient Surveys | ~43,000 (30% of completed visits) | Aurora DSQL |

### Simulated Data Quality Challenges

The upstream engine deliberately injects realistic data quality issues so the pipeline stress-tests against the kind of messy data real healthcare systems produce — not pristine toy data. The pipeline handles all eleven issues below using a **flag-don't-reject** strategy, preserving the raw signal so downstream consumers can decide how to handle it:

| Category | Issue | Rate | Pipeline Handling |
|----------|-------|------|-------------------|
| Appointment Status | No-call no-shows | ~1% | `is_no_show` boolean derived from source `status` column |
| Appointment Status | Cancellations | ~9% | `is_cancelled` boolean derived from source `status` column |
| Missing Vitals | Null height, weight, oxygen saturation | ~3% | NULLs preserved; excluded from averages in Gold |
| Vital Outliers | Fever, hypertensive crisis, severe arrhythmias, hypoxemia | ~2% | `_is_outlier` flag added; Gold models can include or exclude |
| Temporal | Late arrivals (check-in after appointment time) | ~2% | `is_late_arrival` + `late_minutes` derived columns |
| Identity | Duplicate SSNs across patients | ~2% | `_has_duplicate_ssn` flag; SCD2 dedup tolerates collisions |
| Identity | Duplicate email addresses | ~3% | `_has_duplicate_email` flag |
| Identity | Duplicate insurance policy numbers | ~1% | `_has_duplicate_policy` flag |
| Clinical | Diagnosis ↔ vitals contradictions | random | `_has_clinical_contradiction` flag; intentional, not corrected |
| Temporal | Stale `age` column (computed once, never updated) | 100% | Age re-derived from `date_of_birth` in Silver |
| Schema | Middle name sparsity (now ~40% populated) | ~40% | NULL → populated transitions trigger SCD2 version change |

A dedicated `data_quality_report` Gold table surfaces outlier counts, null rates, duplicate counts, and status distribution per pipeline run — demonstrating pipeline observability at the dataset level.

### Medallion Layers

**Bronze** — Raw ingestion into Iceberg tables. Aurora tables read via JDBC (full snapshot for small reference tables, incremental for large tables). DynamoDB tables exported and loaded. Each record tagged with `_source_system`, `_ingested_at`, and `_batch_id`.

**Silver** — Cleaned, conformed, and deduplicated Iceberg tables:

| Table | Type | Key Transformations |
|-------|------|-------------------|
| `dim_patient` | Dimension (SCD2) | Slowly changing dimension with `valid_from` / `valid_to` |
| `dim_provider` | Dimension | Standardized specialties and license types |
| `dim_clinic` | Dimension | 10 SE US locations with region grouping |
| `dim_insurance` | Dimension | Plan + company joined into single dimension |
| `dim_date` | Dimension | Standard calendar dimension for date analytics |
| `fact_appointment` | Fact | `status` column maps to `is_no_show` / `is_cancelled` booleans |
| `fact_visit` | Fact | `appointment_duration_minutes`, denormalized `insurance_plan_id`, Aurora/DynamoDB dedup |
| `fact_vitals` | Fact | Blood pressure parsed to `systolic_bp`/`diastolic_bp`, field name normalization (`oxygen` -> `oxygen_saturation`), Aurora/DynamoDB dedup |
| `fact_medical_record` | Fact | Diagnosis and treatment records |
| `fact_billing_line` | Fact | One row per CPT line item with RVU snapshots, conversion factor, expected_amount (Medicare RVU economics) |
| `fact_survey` | Fact | Patient satisfaction scores (gene_prissy_score, experience_score, free-text feedback) |
| `dim_cpt_code` | Dimension | Medicare CPT reference with work_rvu, pe_rvu, mp_rvu, category |
| `bridge_patient_insurance` | Bridge | Patient-to-insurance-plan many-to-many |
| `bridge_provider_clinic` | Bridge | Provider-to-clinic relationships from clinic schedules |

**Gold** — Business analytics as Redshift-native tables (read from Silver Iceberg via Spectrum), built by dbt-redshift:

| Model | Business Question |
|-------|-------------------|
| `clinic_daily_metrics` | How many appointments per clinic per day? What's the no-show rate and average wait time? |
| `clinic_monthly_summary` | What are the month-over-month trends? Where is capacity being wasted? |
| `patient_risk_profile` | Which patients show deteriorating cardiac indicators based on vitals trends? |
| `patient_cohort_analysis` | How do outcomes compare across age brackets, insurance types, and clinics? |
| `insurance_plan_metrics` | Which plans drive the most volume? What's the visit density per plan? |
| `provider_workload` | Are providers overloaded? How does actual duration compare to scheduled time? |
| `provider_rvu_productivity` | What is each provider's daily wRVU output and expected collections? |
| `revenue_by_payer` | Which insurance companies drive the most expected revenue per clinic? |
| `clinic_financial_performance` | Which clinics have the best expected collections per visit and procedure mix? |
| `patient_satisfaction_trends` | How are satisfaction scores trending per clinic/provider over time? |

### Pipeline Orchestration

```
full_pipeline (Airflow DAG)
│
├── Populate: VitalFold Engine API calls
│   ├── Authenticate (JWT)
│   ├── Static Populate → Aurora reference data
│   ├── Dynamic Populate → Aurora appointments/visits/vitals
│   ├── DynamoDB Sync → Write visits to DynamoDB
│   └── Verify row counts
│
├── Bronze Ingestion (Glue Spark)
│   ├── Ingest 15 Aurora tables via JDBC
│   └── Ingest 2 DynamoDB tables via export
│
├── Silver Transformation (Glue Spark)
│   ├── Build dimensions (SCD2, reference, CPT, calendar)
│   ├── Build facts (dedup, derive metrics, parse fields, RVU snapshots)
│   └── Data quality assertions
│
├── Gold Aggregation (dbt-redshift via Spectrum)
│   ├── dbt run (10 analytics models)
│   └── dbt test (quality checks)
│
└── Quality Report
    └── dbt test (cross-layer validation)
```

## Project Structure

```
vitalFoldProject/
├── docker-compose.yml             # Airflow 3.0 + Superset — single `docker compose up`
├── .env.example                   # AWS config template (AWS_ACCOUNT_ID, AWS_REGION, bucket names)
├── Makefile                       # Common commands: up, down, test, deploy
├── pyproject.toml                 # Python dependencies
│
├── docker/
│   └── airflow/
│       ├── Dockerfile                # Custom Airflow image with Spark, Iceberg JARs, dbt + AWS providers
│       ├── spark-defaults.conf.template  # Spark config with ${VAR} placeholders (envsubst at startup)
│       └── entrypoint.sh            # Runs envsubst to resolve spark-defaults.conf from .env vars
│
├── airflow/dags/
│   ├── vitalfold_populate.py      # Data population via Engine API
│   ├── vitalfold_daily_sync.py    # Daily incremental DynamoDB sync
│   ├── bronze_ingestion.py        # Aurora + DynamoDB → Bronze Iceberg
│   ├── silver_transform.py        # Bronze → Silver Iceberg
│   ├── gold_aggregate.py          # Silver Iceberg → Gold Redshift (dbt-redshift)
│   └── full_pipeline.py           # Master DAG chaining all phases
│
├── spark/
│   ├── jobs/
│   │   ├── bronze/                # Glue ingestion jobs
│   │   ├── silver/                # Glue transformation jobs
│   │   └── gold/                  # Heavy aggregations (if needed)
│   ├── lib/                       # Shared utilities + quality checks
│   └── tests/                     # PySpark unit tests
│
├── dbt/
│   ├── models/
│   │   ├── staging/               # Thin views over Silver Iceberg tables
│   │   └── marts/                 # Gold-layer analytics models
│   ├── macros/
│   └── tests/                     # Custom data quality tests
│
├── terraform/
│   └── modules/                   # s3, glue, redshift, iam, networking
│
├── scripts/                       # Bootstrap, deploy, seed connections
│
└── docs/
    ├── architecture.md            # Detailed architecture and design decisions
    ├── data_dictionary.md         # Every table, every column, every layer
    ├── airflow-integration.md     # VitalFold Engine API reference + example DAGs
    ├── setup_guide.md             # Environment setup for new developers
    ├── runbook.md                 # Ops guide: resets, re-runs, troubleshooting
    ├── cost_log.md                # Actual AWS cost tracking
    └── schemas/                   # Aurora DSQL + DynamoDB schemas
```

## Quick Start

### Prerequisites

- Docker and Docker Compose
- AWS account with credentials configured
- Terraform >= 1.5
- Running VitalFold Engine instance ([TRO-Wolf/VitalFoldSimulator](https://github.com/TRO-Wolf/VitalFoldSimulator))

### Setup

```bash
# Clone and enter the project
git clone https://github.com/TRO-Wolf/vitalFoldProject.git
cd vitalFoldProject

# Configure environment (AWS account, region, S3 bucket names)
cp .env.example .env
# Edit .env with your AWS account ID, region, and S3 bucket names

# Provision AWS infrastructure
cd terraform && terraform init && terraform apply
cd ..

# Start Airflow and Superset
docker compose up -d

# Seed Airflow connections and variables
./scripts/seed_connections.sh

# Open Airflow UI
open http://localhost:8080
```

### Run the Pipeline

1. Trigger the `full_pipeline` DAG from the Airflow UI
2. Monitor progress through Airflow's task logs
3. Query Gold tables in the Redshift Query Editor v2
4. View dashboards at `http://localhost:8088` (Superset)

For detailed setup instructions, see [docs/setup_guide.md](docs/setup_guide.md).

## Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Spark for Bronze/Silver, dbt-redshift for Gold | Split by workload type | Spark handles heavy JDBC extraction and cross-source dedup; dbt handles SQL aggregations against Redshift. Redshift Spectrum bridges Iceberg and warehouse. Demonstrates proficiency in Spark, Iceberg, AND Redshift. |
| Iceberg over Delta Lake / Hudi | Open standard, broadest support | 78% industry adoption (2026). Works across Spark, Athena, Trino, Flink, Redshift Spectrum. |
| Glue 5.1 over EMR Serverless | Operational simplicity | Zero infrastructure to manage. Native Iceberg V3 support. Glue Catalog integration. |
| Standard S3 + Iceberg over S3 Tables | Cost and portability | S3 Tables is 36% more expensive. Standard Iceberg-on-S3 demonstrates deeper understanding of the table format internals. |
| Iceberg for Bronze/Silver, Redshift-native for Gold | Right tool per layer | Silver stays open-format and portable. Gold is Redshift-native for fast dashboard queries. Spectrum external schema connects them. |
| Redshift Serverless over Provisioned | Scales to zero | Only charges per RPU-hour when queries run. No cluster management. Shows Redshift skills without idle costs. |
| Airflow 3.0 over Dagster/Prefect | Industry standard | 30M+ monthly downloads, 80K+ organizations. Mirrors production MWAA deployments. Data Assets feature shows modern 3.0 expertise. |
| No streaming layer | Honest to the data | Source data is batch-generated. Adding Kafka/Kinesis would be over-engineering. DynamoDB Streams is a documented stretch goal. |

## Cost

Estimated ~$55-75/month (excluding Aurora DSQL which is pre-existing):

| Service | Est. Monthly Cost |
|---------|------------------|
| Glue Jobs (7-9 jobs, 2 DPU each) | $40-55 |
| Redshift Serverless (scales to zero when idle) | $5-15 |
| S3 Storage (~15 GB across Bronze/Silver) | $0.35 |
| S3 Requests + Iceberg overhead | ~$1 |
| Airflow + Superset (local Docker) | $0 |

## Documentation

| Document | Description |
|----------|-------------|
| [Architecture](docs/architecture.md) | Data flow, technology choices, medallion layer design |
| [Data Dictionary](docs/data_dictionary.md) | Every table and column across Bronze, Silver, and Gold |
| [API Integration](docs/airflow-integration.md) | VitalFold Engine API reference with Airflow DAG examples |
| [Setup Guide](docs/setup_guide.md) | Environment configuration for new developers |
| [Runbook](docs/runbook.md) | Operational procedures: resets, re-runs, troubleshooting |
| [Cost Log](docs/cost_log.md) | Tracked AWS costs with actual Glue DPU measurements |

## Related

- **[VitalFold Engine](https://github.com/TRO-Wolf/VitalFoldSimulator)** — The Rust/Actix-web simulation engine that generates the source data. Public repo. Provides 22 REST endpoints for populating Aurora DSQL (15 tables in `vital_fold` schema) and syncing visit/vitals data to DynamoDB. Ships with Medicare CPT/RVU billing reference data (CY2024 conversion factor $32.7442) and patient satisfaction surveys, enabling real healthcare finance analytics.

