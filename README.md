# VitalFold Data Pipeline

An end-to-end data engineering pipeline that ingests synthetic healthcare data from a dual-source system (Aurora DSQL + DynamoDB), transforms it through a medallion architecture, and serves analytics dashboards — all orchestrated by Airflow and built on Apache Iceberg.

## What This Project Demonstrates

- **Medallion architecture** (Bronze / Silver / Gold) on Apache Iceberg tables in S3
- **Dual-source ingestion** from Aurora DSQL (13 relational tables) and DynamoDB (2 NoSQL tables) with cross-source deduplication
- **Dimensional modeling** with SCD Type 2 slowly changing dimensions, derived metrics, and bridge tables
- **Orchestration** with Apache Airflow 3.0, including API-driven data population and Glue job management
- **Transformations** split by purpose: PySpark (Glue) for heavy Bronze/Silver ETL, dbt for Gold-layer SQL analytics
- **Infrastructure as Code** via Terraform (S3, Glue, IAM, Data Catalog)
- **Data quality** enforced at every layer through dbt tests and PySpark assertions

## Architecture

```
                        VitalFold Engine API
                 (Rust, Actix-web — separate repo)
                               │
          ┌────────────────────┼────────────────────┐
          v                    v                    v
   POST /populate/static  POST /populate/dynamic  POST /simulate/date-range
          │                    │                    │
          v                    v                    v
   Aurora DSQL             Aurora DSQL           DynamoDB
   (reference data)        (appointments,        (patient_visit,
                            visits, vitals)       patient_vitals)
          │                    │                    │
          └────────┬───────────┘                    │
                   v                                v
            Glue Spark (JDBC)                 Glue Spark (Export)
                   │                                │
                   └──────────┬─────────────────────┘
                              v
                    ┌─────────────────┐
                    │  S3 Bronze      │  Raw Iceberg tables
                    │  (15 tables)    │  + metadata columns
                    └────────┬────────┘
                             v
                    ┌─────────────────┐
                    │  Glue Spark     │  Clean, dedup, conform
                    │  (Silver ETL)   │  SCD2, BP parsing, no-show flags
                    └────────┬────────┘
                             v
                    ┌─────────────────┐
                    │  S3 Silver      │  Iceberg dims, facts, bridges
                    │  (11 tables)    │  Glue Data Catalog
                    └────────┬────────┘
                             v
                    ┌─────────────────┐
                    │  dbt + Athena   │  SQL aggregations
                    │  (Gold ETL)     │  Business metrics
                    └────────┬────────┘
                             v
                    ┌─────────────────┐
                    │  S3 Gold        │  Iceberg analytics tables
                    │  (6 models)     │  Glue Data Catalog
                    └────────┬────────┘
                             v
                    ┌─────────────────┐
                    │  Superset       │  Dashboards via Athena
                    └─────────────────┘

   Orchestration: Airflow 3.0 (local Docker Compose)
   Infrastructure: Terraform
```

## Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| Data Source | VitalFold Engine (Rust/Actix-web) | Generates 500K+ synthetic cardiac clinic records |
| Primary Storage | Aurora DSQL (PostgreSQL-compatible) | 13 relational tables — patients, providers, appointments, vitals |
| Event Storage | DynamoDB | 2 tables — real-time visit and vitals capture |
| Orchestration | Apache Airflow 3.0 | DAG-based pipeline orchestration with Data Assets |
| Batch Processing | AWS Glue 5.1 (Spark 3.5.2) | Bronze/Silver ETL with native Iceberg V3 support |
| Table Format | Apache Iceberg (v1.10, Format V3) | ACID transactions, schema evolution, time travel |
| Catalog | AWS Glue Data Catalog | Iceberg metadata catalog for all layers |
| Transformations | dbt-core + dbt-athena | Gold-layer SQL models, testing, documentation |
| Query Engine | Amazon Athena | Serverless SQL over Iceberg tables ($5/TB scanned) |
| Dashboards | Apache Superset | Interactive analytics dashboards |
| Infrastructure | Terraform | S3 buckets, Glue jobs, IAM roles, Data Catalog |
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
| Appointments | ~100,000 | Aurora DSQL |
| Medical Records | ~100,000 | Aurora DSQL |
| Patient Visits | ~100,000 | Aurora DSQL + DynamoDB |
| Patient Vitals | ~100,000 | Aurora DSQL + DynamoDB |

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
| `fact_appointment` | Fact | Derived `no_show_flag` (LEFT JOIN to visits) |
| `fact_visit` | Fact | `appointment_duration_minutes`, denormalized `insurance_plan_id`, Aurora/DynamoDB dedup |
| `fact_vitals` | Fact | Blood pressure parsed to `systolic_bp`/`diastolic_bp`, field name normalization (`oxygen` -> `oxygen_saturation`), Aurora/DynamoDB dedup |
| `fact_medical_record` | Fact | Diagnosis and treatment records |
| `bridge_patient_insurance` | Bridge | Patient-to-insurance-plan many-to-many |
| `bridge_provider_clinic` | Bridge | Provider-to-clinic relationships from clinic schedules |

**Gold** — Business analytics as Iceberg tables, built by dbt:

| Model | Business Question |
|-------|-------------------|
| `clinic_daily_metrics` | How many appointments per clinic per day? What's the no-show rate and average wait time? |
| `clinic_monthly_summary` | What are the month-over-month trends? Where is capacity being wasted? |
| `patient_risk_profile` | Which patients show deteriorating cardiac indicators based on vitals trends? |
| `patient_cohort_analysis` | How do outcomes compare across age brackets, insurance types, and clinics? |
| `insurance_plan_metrics` | Which plans drive the most volume? What's the visit density per plan? |
| `provider_workload` | Are providers overloaded? How does actual duration compare to scheduled time? |

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
│   ├── Ingest 13 Aurora tables via JDBC
│   └── Ingest 2 DynamoDB tables via export
│
├── Silver Transformation (Glue Spark)
│   ├── Build dimensions (SCD2, reference, calendar)
│   ├── Build facts (dedup, derive metrics, parse fields)
│   └── Data quality assertions
│
├── Gold Aggregation (dbt via Athena)
│   ├── dbt run (6 analytics models)
│   └── dbt test (quality checks)
│
└── Quality Report
    └── dbt test (cross-layer validation)
```

## Project Structure

```
vitalFoldProject/
├── docker-compose.yml             # Airflow 3.0 + Superset — single `docker compose up`
├── .env.example                   # Environment variables template
├── Makefile                       # Common commands: up, down, test, deploy
├── pyproject.toml                 # Python dependencies
│
├── docker/
│   └── airflow/Dockerfile         # Custom Airflow image with dbt + AWS providers
│
├── airflow/dags/
│   ├── vitalfold_populate.py      # Data population via Engine API
│   ├── vitalfold_daily_sync.py    # Daily incremental DynamoDB sync
│   ├── bronze_ingestion.py        # Aurora + DynamoDB → Bronze Iceberg
│   ├── silver_transform.py        # Bronze → Silver Iceberg
│   ├── gold_aggregate.py          # Silver → Gold Iceberg (dbt)
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
│   └── modules/                   # s3, glue, iam, networking
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
- Running VitalFold Engine instance ([separate repo](https://github.com/your-username/vitalFoldEngine))

### Setup

```bash
# Clone and enter the project
git clone https://github.com/your-username/vitalFoldProject.git
cd vitalFoldProject

# Configure environment
cp .env.example .env
# Edit .env with your AWS credentials and VitalFold Engine URL

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
3. Query Gold tables in the Athena console
4. View dashboards at `http://localhost:8088` (Superset)

For detailed setup instructions, see [docs/setup_guide.md](docs/setup_guide.md).

## Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Spark for Bronze/Silver, dbt for Gold | Split by workload type | Spark handles heavy JDBC extraction and cross-source dedup; dbt handles SQL aggregations. Demonstrates proficiency in both. |
| Iceberg over Delta Lake / Hudi | Open standard, broadest support | 78% industry adoption (2026). Works across Spark, Athena, Trino, Flink. AWS has invested heavily in Iceberg across Glue, Athena, and EMR. |
| Glue 5.1 over EMR Serverless | Operational simplicity | Zero infrastructure to manage. Native Iceberg V3 support. Glue Catalog integration. |
| Standard S3 + Iceberg over S3 Tables | Cost and portability | S3 Tables is 36% more expensive. Standard Iceberg-on-S3 demonstrates deeper understanding of the table format internals. |
| Athena + dbt over Redshift Serverless | Cost efficiency | Athena is $5/TB scanned (pennies at this scale). Redshift Serverless minimum is $2.88/hr. The SQL skills transfer directly. |
| Airflow 3.0 over Dagster/Prefect | Industry standard | 30M+ monthly downloads, 80K+ organizations. Mirrors production MWAA deployments. Data Assets feature shows modern 3.0 expertise. |
| No streaming layer | Honest to the data | Source data is batch-generated. Adding Kafka/Kinesis would be over-engineering. DynamoDB Streams is a documented stretch goal. |

## Cost

Estimated ~$50-60/month (excluding Aurora DSQL which is pre-existing):

| Service | Est. Monthly Cost |
|---------|------------------|
| Glue Jobs (6-7 jobs, 2 DPU each) | $40-55 |
| Athena (dbt runs + ad-hoc queries) | $0.50-3 |
| S3 Storage (~15 GB across all layers) | $0.35 |
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

- **VitalFold Engine** — The Rust-based simulation engine that generates the source data. Built with Actix-web, it provides a REST API for populating Aurora DSQL and syncing to DynamoDB.
