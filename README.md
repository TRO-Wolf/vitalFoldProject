# VitalFold Data Pipeline

> **In plain English:** A data pipeline for a fictional network of 10 cardiac clinics. It pulls raw patient visits, vital signs, and billing records from two different databases, cleans and conforms them through a medallion architecture, and produces analytics-ready Iceberg tables for executive dashboards covering revenue, provider productivity, patient outcomes, and satisfaction trends. Same medallion + Iceberg + dbt stack used by modern lakehouse data platforms.

[![Status](https://img.shields.io/badge/status-portfolio-3B82F6?style=flat-square)](docs/portfolio-gaps.md)
[![License: MIT](https://img.shields.io/badge/license-MIT-22C55E?style=flat-square)](LICENSE)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=flat-square&logo=apacheairflow&logoColor=white)](https://airflow.apache.org)
[![dbt](https://img.shields.io/badge/dbt--spark-FF694B?style=flat-square&logo=dbt&logoColor=white)](https://www.getdbt.com)
[![Astronomer Cosmos](https://img.shields.io/badge/Astronomer%20Cosmos-7C3AED?style=flat-square)](https://astronomer.github.io/astronomer-cosmos/)
[![Apache Iceberg](https://img.shields.io/badge/Apache%20Iceberg-1E90FF?style=flat-square&logo=apacheiceberg&logoColor=white)](https://iceberg.apache.org)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=flat-square&logo=apachespark&logoColor=white)](https://spark.apache.org)
[![Polars](https://img.shields.io/badge/Polars-CD792C?style=flat-square&logo=polars&logoColor=white)](https://pola.rs)
[![Python](https://img.shields.io/badge/Python-3776AB?style=flat-square&logo=python&logoColor=white)](https://www.python.org)
[![Aurora DSQL](https://img.shields.io/badge/Aurora%20DSQL-4169E1?style=flat-square&logo=amazonaurora&logoColor=white)](https://aws.amazon.com/rds/aurora/dsql/)
[![DynamoDB](https://img.shields.io/badge/DynamoDB-4053D6?style=flat-square&logo=amazondynamodb&logoColor=white)](https://aws.amazon.com/dynamodb/)
[![AWS Glue](https://img.shields.io/badge/AWS%20Glue-FF9900?style=flat-square&logo=amazonaws&logoColor=white)](https://aws.amazon.com/glue/)
[![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat-square&logo=docker&logoColor=white)](https://www.docker.com)

---

## Table of Contents

- [VitalFold Data Pipeline](#vitalfold-data-pipeline)
  - [Table of Contents](#table-of-contents)
  - [Business Outcomes](#business-outcomes)
  - [What This Project Demonstrates](#what-this-project-demonstrates)
  - [Architecture](#architecture)
  - [Tech Stack](#tech-stack)
  - [Data Pipeline](#data-pipeline)
    - [Source Data](#source-data)
    - [Simulated Data Quality Challenges](#simulated-data-quality-challenges)
    - [Medallion Layers](#medallion-layers)
    - [Pipeline Orchestration](#pipeline-orchestration)
  - [Project Structure](#project-structure)
  - [Quick Start](#quick-start)
    - [Prerequisites](#prerequisites)
    - [Setup](#setup)
    - [Airflow Connections (one-time, via UI or CLI)](#airflow-connections-one-time-via-ui-or-cli)
    - [Run the Pipeline](#run-the-pipeline)
  - [Design Decisions](#design-decisions)
  - [Documentation](#documentation)
  - [Related](#related)
  - [License](#license)

---

## Business Outcomes

The pipeline answers real questions a cardiac clinic operator would ask:

| Theme | Questions Answered |
|-------|--------------------|
| **Operations** | How many appointments per clinic per day? What's the no-show rate? Are providers overloaded? |
| **Clinical** | Which patients show deteriorating cardiac indicators? How do outcomes differ by age and insurance? |
| **Finance** | What's each provider's daily wRVU output and expected collections? Which insurance companies drive the most revenue? Which clinics perform best financially? |
| **Satisfaction** | How are patient satisfaction scores trending per clinic and provider? |

All metrics are built on real Medicare RVU economics (CY2024 conversion factor $32.7442), not made-up numbers.

## What This Project Demonstrates

- **Medallion architecture** (Bronze / Silver / Gold) on Apache Iceberg tables in S3 via the AWS Glue Data Catalog
- **Dual-source ingestion** from Aurora DSQL (15 relational tables) and DynamoDB (2 NoSQL tables) with cross-source deduplication
- **Dimensional modeling** with SCD Type 2 slowly changing dimensions, derived metrics, and bridge tables
- **Orchestration** with Apache Airflow 3.1 — Asset-triggered medallion handoffs (silver completion fires gold dbt build), custom hooks, and Astronomer Cosmos for per-model dbt task rendering
- **Custom Airflow components**: [`DSQLSqlHook`](airflow/includes/hooks/dsql.py) (psycopg + SQLAlchemy + IAM token auth) and [`DSQLToS3Operator`](airflow/includes/operators/dsql_to_s3.py) (polars + parquet) for Aurora DSQL → S3 extraction
- **Transformations** split by purpose: PySpark for heavy Bronze/Silver ETL (JDBC reads, cross-source dedup, SCD2), dbt-spark for Gold-layer SQL aggregations against Iceberg via Spark Thrift Server
- **Open-format end-to-end**: every layer (Bronze, Silver, Gold) is Iceberg in the Glue Data Catalog — same engine reads/writes throughout
- **Data quality** enforced via dbt tests and inline Spark assertions
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
          DSQLToS3Operator (JDBC)            Spark export reader
          (custom Airflow operator)                 │
                   │                                │
                   └──────────┬─────────────────────┘
                              v
                    ┌─────────────────┐
                    │  S3 Bronze      │  Raw Iceberg tables
                    │  (15 Aurora +   │  + metadata columns
                    │   2 DynamoDB)   │  Glue Data Catalog
                    └────────┬────────┘
                             v
                    ┌─────────────────┐
                    │  Spark          │  Clean, dedup, conform
                    │  (Silver ETL)   │  SCD2, BP parsing, RVU mapping
                    └────────┬────────┘
                             v
                    ┌─────────────────┐
                    │  S3 Silver      │  Iceberg dims, facts, bridges
                    │  (14 tables)    │  Glue Data Catalog
                    └────────┬────────┘   emits Airflow Asset
                             v          vital_fold://silver/facts
                    ┌─────────────────────────┐
                    │  Spark Thrift Server    │  Read/write via JDBC
                    │  (spark.sql.defaultCatalog = glue_catalog)
                    └────────┬────────────────┘
                             v
                    ┌─────────────────┐
                    │  dbt-spark      │  Iceberg-format models
                    │  (Gold ETL)     │  via Cosmos DbtTaskGroup
                    └────────┬────────┘
                             v
                    ┌─────────────────┐
                    │  S3 Gold        │  Iceberg analytics tables
                    │  vital_fold_gold│  Glue Data Catalog
                    └────────┬────────┘
                             v
                    ┌─────────────────┐
                    │  Athena / BI    │  Query layer
                    └─────────────────┘

   Orchestration: Airflow 3.1 (local Docker Compose, CeleryExecutor)
```

## Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| Data Source | [VitalFold Engine](https://github.com/TRO-Wolf/VitalFoldSimulator) (Rust/Actix-web) | Generates 750K+ synthetic cardiac clinic records including Medicare RVU billing and satisfaction surveys |
| Primary Storage | Aurora DSQL (PostgreSQL-compatible) | 15 relational tables — patients, providers, appointments, vitals, billing, surveys |
| Event Storage | DynamoDB | 2 tables — real-time visit and vitals capture |
| Orchestration | Apache Airflow 3.1 (CeleryExecutor) | DAG-based pipeline orchestration with Data Assets for medallion-layer handoffs |
| Batch Processing | Apache Spark 3.5 | Bronze/Silver ETL via SparkSubmitOperator; dbt-spark executes against Spark Thrift Server |
| Table Format (all layers) | Apache Iceberg v1.10 (Format V2) | ACID transactions, schema evolution, time travel — used uniformly Bronze → Silver → Gold |
| Catalog | AWS Glue Data Catalog | Iceberg metadata catalog for all medallion layers |
| Transformations (Bronze/Silver) | PySpark ([`spark/scripts/`](spark/scripts/)) | Cross-source dedup, SCD2, vital parsing, quality flags |
| Transformations (Gold) | dbt-core + dbt-spark ([`dbt/`](dbt/)) | SQL models materialized as Iceberg tables; tests + docs |
| Task Rendering | Astronomer Cosmos | Renders each dbt model as its own Airflow task with per-model retries and logs |
| Custom Components | [`DSQLSqlHook`](airflow/includes/hooks/dsql.py), [`DSQLToS3Operator`](airflow/includes/operators/dsql_to_s3.py) | Aurora DSQL JDBC + polars + parquet extraction; IAM token auth via boto3 |
| Container Stack | Docker Compose | Airflow 3.1 (apiserver, scheduler, worker, triggerer, dag-processor) + Postgres + Redis |

**Planned (not yet wired):** Spark Thrift Server service in compose, Terraform modules for the AWS side, Apache Superset dashboards, GitHub Actions CI.

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

A dedicated `data_quality_report` Gold table (planned) will surface outlier counts, null rates, duplicate counts, and status distribution per pipeline run — demonstrating pipeline observability at the dataset level.

### Medallion Layers

**Bronze** — Raw ingestion into Iceberg tables. Aurora tables read via JDBC through the custom [`DSQLToS3Operator`](airflow/includes/operators/dsql_to_s3.py) (polars-backed, parquet output). DynamoDB tables exported and loaded. Each record tagged with `_source_system`, `_ingested_at`, and `_batch_id`.

**Silver** — Cleaned, conformed, and deduplicated Iceberg tables produced by [`spark/scripts/process_silver.py`](spark/scripts/process_silver.py) and [`spark/scripts/silver_dim_jobs.py`](spark/scripts/silver_dim_jobs.py):

| Table | Type | Key Transformations |
|-------|------|-------------------|
| `dim_patient` | Dimension (SCD2) | Slowly changing dimension with `valid_from` / `valid_to` |
| `dim_provider` | Dimension | Standardized specialties and license types |
| `dim_clinic` | Dimension | 10 SE US locations with region grouping |
| `dim_insurance` | Dimension | Plan + company joined into single dimension |
| `dim_dates` | Dimension | Standard calendar dimension for date analytics |
| `fact_appointment` | Fact | `status` column maps to `is_no_show` / `is_cancelled` booleans |
| `fact_visit` | Fact | `appointment_duration_minutes`, denormalized `insurance_plan_id`, Aurora/DynamoDB dedup |
| `fact_vitals` | Fact | Blood pressure parsed to `systolic_bp`/`diastolic_bp`, field name normalization (`oxygen` → `oxygen_saturation`), Aurora/DynamoDB dedup |
| `fact_medical_record` | Fact | Diagnosis and treatment records |
| `fact_billing_line` | Fact | One row per CPT line item with RVU snapshots, conversion factor, expected_amount (Medicare RVU economics) |
| `fact_survey` | Fact | Patient satisfaction scores (gene_prissy_score, experience_score, free-text feedback) |
| `dim_cpt_code` | Dimension | Medicare CPT reference with work_rvu, pe_rvu, mp_rvu, category |
| `bridge_patient_insurance` | Bridge | Patient-to-insurance-plan many-to-many |
| `bridge_provider_clinic` | Bridge | Provider-to-clinic relationships from clinic schedules |

**Gold** — Business analytics as Iceberg tables in the `vital_fold_gold` namespace, built by **dbt-spark** through Spark Thrift Server. Each model declares `{{ config(materialized='table', file_format='iceberg') }}` and references Silver via `{{ source('vital_fold_silver', '...') }}`. Currently shipped:

| Model | Business Question |
|-------|-------------------|
| [`fct_survey_visit`](dbt/models/vital_fold/fct_survey_visit.sql) | One row per survey response with provider/visit/appointment/date dims joined; derives `wait_time_minutes` from checkin → provider-seen times |
| [`agg_clinic_daily_experience`](dbt/models/vital_fold/agg_clinic_daily_experience.sql) | Clinic × calendar-date rollup — average experience/gene-prissy/wait-time scores plus survey and visit counts |

Additional Gold models for finance (RVU productivity, revenue by payer, clinic financial performance), operations (clinic daily metrics, provider workload), and clinical (patient risk profile, cohort analysis) are planned — the dbt-spark + Iceberg pattern is proven and extending it is incremental.

### Pipeline Orchestration

```
vf_bronze_extraction_dag                   Daily at 05:00 US/Eastern
└── DSQLToS3Operator × 6 tables            Aurora DSQL → S3 Bronze (parquet)

vf_silver_dag                              (planned — closes the chain)
├── process_silver  (SparkSubmit)          Bronze → Silver Iceberg
└── silver_dim_jobs (SparkSubmit)          Build dim_dates and other dims
    └── emits Asset("vital_fold://silver/facts")
                                                       │
                                                       v
vf_gold_dbt_pipeline                       Triggered by silver Asset
├── BashOperator: dbt deps                 Install dbt-utils
├── BashOperator: dbt run --select vital_fold
└── BashOperator: dbt test  --select vital_fold

vf_gold_dbt_cosmos_pipeline                Cosmos-rendered alternative
└── DbtTaskGroup                           One Airflow task per dbt model
    (LoadMode.DBT_MANIFEST so DAG parse never hits Thrift)

vf_object_deletion_dag                     Manual — S3 cleanup utility
└── Batched DeleteObjects (1000-key pages)
```

DAG files: [`vf_bronze_extraction_dag.py`](airflow/dags/vital_fold/vf_bronze_extraction_dag.py) · [`vf_gold_dbt_dag.py`](airflow/dags/vital_fold/vf_gold_dbt_dag.py) · [`vf_gold_dbt_cosmos_dag.py`](airflow/dags/vital_fold/vf_gold_dbt_cosmos_dag.py) · [`vf_object_deletion_dag.py`](airflow/dags/vital_fold/vf_object_deletion_dag.py).

## Project Structure

```
vitalFoldProject/
├── README.md                       # This file
├── LICENSE                         # MIT
├── CLAUDE.md                       # Internal dev notes (architecture deep-dive, decisions)
├── .env.example                    # AWS_ACCOUNT_ID, AWS_REGION, bucket names, AIRFLOW_PROJ_DIR
├── .gitignore                      # Ignores .env and .claude/
│
├── docker/
│   └── airflow/
│       ├── Dockerfile              # Custom Airflow 3.1.3 image (Spark 3.5, Iceberg JARs, dbt-spark, Cosmos)
│       ├── docker-compose.yaml     # Airflow stack: apiserver, scheduler, worker, triggerer, dag-processor, postgres, redis
│       ├── requirements.txt        # dbt-core, dbt-spark, astronomer-cosmos, providers, polars, etc.
│       ├── spark-defaults.conf.template  # Spark config with ${VAR} placeholders
│       └── entrypoint.sh           # envsubst at container start
│
├── airflow/
│   ├── dags/
│   │   └── vital_fold/
│   │       ├── vf_bronze_extraction_dag.py    # Aurora DSQL → S3 Bronze
│   │       ├── vf_gold_dbt_dag.py             # BashOperator dbt pipeline
│   │       ├── vf_gold_dbt_cosmos_dag.py      # Cosmos DbtTaskGroup version
│   │       └── vf_object_deletion_dag.py      # S3 cleanup utility
│   ├── includes/
│   │   ├── hooks/dsql.py           # DSQLSqlHook (psycopg + SQLAlchemy + IAM token)
│   │   ├── operators/dsql_to_s3.py # DSQLToS3Operator (polars / parquet)
│   │   └── sql/vital_fold/bronze/*.sql  # 6 bronze extraction templates
│   └── plugins/files/              # AWS RDS / Aurora root CA certs
│
├── spark/
│   └── scripts/
│       ├── process_silver.py       # Bronze → Silver Iceberg (fact + dim cleanup)
│       ├── silver_dim_jobs.py      # Builds dim_dates and other silver dimensions
│       ├── run_sql.py              # Generic SQL runner (Iceberg DDL, MERGE INTO)
│       └── utils/spark_file_tools.py
│
├── dbt/
│   ├── dbt_project.yml             # profile: vitalfold, default Iceberg materialization
│   ├── profiles.yml                # glue_spark target → spark-thrift:10000 (Thrift type, no auth)
│   ├── packages.yml                # dbt-utils ^1.0.0
│   ├── package-lock.yml
│   └── models/vital_fold/
│       ├── fct_survey_visit.sql
│       ├── agg_clinic_daily_experience.sql
│       ├── _models.yml             # tests + column docs
│       └── _sources.yml            # vital_fold_silver source declarations
│
└── docs/
    ├── airflow-integration.md      # VitalFold Engine API reference for Airflow DAGs
    └── portfolio-gaps.md           # Working doc — what's left before fully public
```

## Quick Start

### Prerequisites

- Docker and Docker Compose
- AWS account with credentials (for S3 + Glue Data Catalog)
- Running VitalFold Engine instance ([TRO-Wolf/VitalFoldSimulator](https://github.com/TRO-Wolf/VitalFoldSimulator)) populating an Aurora DSQL cluster you control

### Setup

```bash
# Clone and enter the project
git clone https://github.com/TRO-Wolf/vitalFoldProject.git
cd vitalFoldProject

# Configure environment (AWS account ID, region, bucket names)
cp .env.example .env
# Edit .env with real values

# Build and start the Airflow stack
cd docker/airflow
docker compose build
docker compose up -d

# Open Airflow UI
# http://localhost:8080  (airflow / airflow)
```

### Airflow Connections (one-time, via UI or CLI)

- `vital_fold_dsql` — Aurora DSQL cluster (extras: `default_cluster`, `default_host`)
- `vital_fold_aws` — AWS credentials for S3 writes

### Run the Pipeline

1. From the Airflow UI, un-pause [`vf_bronze_extraction_dag`](airflow/dags/vital_fold/vf_bronze_extraction_dag.py) and trigger a run.
2. Once the silver DAG is wired (see [`docs/portfolio-gaps.md`](docs/portfolio-gaps.md)), it will fire automatically on bronze completion and emit the silver Asset.
3. [`vf_gold_dbt_pipeline`](airflow/dags/vital_fold/vf_gold_dbt_dag.py) consumes the Asset and builds the Gold Iceberg models.
4. Query Gold tables via Athena (Glue Catalog) or any Iceberg-aware engine.

## Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Iceberg everywhere (Bronze, Silver, Gold) | Single open table format end-to-end | Same engine reads/writes all layers; no warehouse round-trip; portable across Spark, Athena, Trino, Flink. |
| Iceberg over Delta Lake / Hudi | Open standard, broadest support | Highest industry adoption; works across the entire AWS analytics surface. |
| dbt-spark for Gold (not dbt-redshift) | Lake-native transforms | Gold stays in the same catalog as Silver; no Spectrum round-trip; one auth model. |
| Astronomer Cosmos for dbt rendering | Per-model Airflow tasks | Each dbt model becomes its own Airflow task with retries, logs, and failure isolation — far better than a single BashOp running the whole project. |
| Custom DSQL hook + operator | First-class Aurora DSQL support | Stock providers don't register DSQL as a conn type; custom hook handles IAM token generation + polars serialization. |
| Airflow 3.1 with Data Assets | Asset-triggered handoffs | Silver completion emits `Asset("vital_fold://silver/facts")` which fires gold DAGs — declarative dependency, no manual schedules. |
| Local Docker stack | Reproducible, portfolio-friendly | One `docker compose up` brings the whole orchestration tier up. AWS-side resources (S3, Glue Catalog, DSQL) live in real AWS. |
| No streaming layer | Honest to the data | Source data is batch-generated. Adding Kafka/Kinesis would be over-engineering for the simulation. |

## Documentation

| Document | Description |
|----------|-------------|
| [docs/airflow-integration.md](docs/airflow-integration.md) | VitalFold Engine API reference with Airflow DAG examples |
| [docs/portfolio-gaps.md](docs/portfolio-gaps.md) | Working punch list — what's still between the current repo and a fully polished public portfolio |
| [CLAUDE.md](CLAUDE.md) | Architecture deep-dive, schema reference, design rationale, implementation roadmap |
| [docs/skills/](docs/skills/) | AI-assistant operating manuals (per model tier) — the engineering discipline this repo is built to |
| [airflow/includes/sql/vital_fold/bronze/](airflow/includes/sql/vital_fold/bronze/) | Bronze SQL extraction templates (6 files) |
| [dbt/models/vital_fold/](dbt/models/vital_fold/) | dbt-spark Gold Iceberg models + tests + source declarations |

## Related

- **[VitalFold Engine](https://github.com/TRO-Wolf/VitalFoldSimulator)** — The Rust/Actix-web simulation engine that generates the source data. Public repo. Provides 22 REST endpoints for populating Aurora DSQL (15 tables in `vital_fold` schema) and syncing visit/vitals data to DynamoDB. Ships with Medicare CPT/RVU billing reference data (CY2024 conversion factor $32.7442) and patient satisfaction surveys, enabling real healthcare finance analytics.

## License

[MIT](LICENSE) © John Huntley

---

<sub>Architecture deep-dive: <a href="CLAUDE.md">CLAUDE.md</a> · Portfolio punch list: <a href="docs/portfolio-gaps.md">docs/portfolio-gaps.md</a> · Engine API reference: <a href="docs/airflow-integration.md">docs/airflow-integration.md</a> · Upstream simulator: <a href="https://github.com/TRO-Wolf/VitalFoldSimulator">TRO-Wolf/VitalFoldSimulator</a>. Code and DDL are the source of truth; this README is a rendered view.</sub>
