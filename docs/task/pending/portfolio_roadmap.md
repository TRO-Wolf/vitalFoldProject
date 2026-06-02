# VitalFold Portfolio — Build Roadmap

> **What this is:** the sequenced, dependency-aware execution plan to take this repo from
> "strong bronze + partial silver/gold" to a complete, reproducible, recruiter-ready
> portfolio. It is the companion to [docs/portfolio-gaps.md](../../portfolio-gaps.md): that
> file is the *inventory* of what's missing; this file is the *order* to build it in. The
> historical Phase 0-5 plan lives in [project_task.md](project_task.md) and is partly
> superseded by the dbt-spark + Iceberg-gold pivot.

---

## Where the project stands today

**Built and committed:**
- Bronze: `vf_bronze_extraction_dag` + custom [`DSQLSqlHook`](../../../airflow/includes/hooks/dsql.py) and [`DSQLToS3Operator`](../../../airflow/includes/operators/dsql_to_s3.py) (Aurora DSQL → S3 parquet), 6 bronze SQL templates.
- Silver: [`process_silver.py`](../../../spark/scripts/process_silver.py) (appointment, appointment_cpt, provider, patient_visit, clinic, survey — dedup + type-cast + MERGE) and [`silver_dim_jobs.py`](../../../spark/scripts/silver_dim_jobs.py) (`dim_dates`).
- Gold: 2 dbt-spark models ([`fct_survey_visit`](../../../dbt/models/vital_fold/fct_survey_visit.sql), [`agg_clinic_daily_experience`](../../../dbt/models/vital_fold/agg_clinic_daily_experience.sql)) + two gold DAGs (BashOperator and Cosmos).
- Platform: Docker Airflow stack, CI lint (ruff + yamllint + dbt parse), README/CLAUDE.md/docs/skills.

**The chain does not yet run end-to-end** — see Milestone 1.

---

## Milestone 0 — Naming & schema normalization (do this first)

Do this before adding more gold models, because every new model that references a silver
column makes a later rename more expensive. See the **Column Naming Verdict** at the bottom
for the full rationale.

- [ ] **Standardize the silver `patient_visit` prefix.** Replace the casual `pv_` abbreviation (`pv_provider_id`, `pv_appointment_id`, `pv_patient_id`, `pv_clinic_id`) with a spelled-out `visit_` prefix in [`process_silver.py`](../../../spark/scripts/process_silver.py). `pv_` violates this repo's own naming manual (docs/skills/ — "no casual abbreviations").
- [ ] **Rename the awkward surrogate key.** `pv_visit_appointment_id_sk` is keyed on `patient_id + appointment_id`, so the name misleads. Rename to `visit_sk` with a header comment documenting its composition.
- [ ] **Pick one audit-column convention and apply it everywhere.** Today silver uses a bare `ingestion_timestamp`; the bronze meta columns dropped are `operation_type` / `dag_version_id` / `run_type`, which don't match the `_source_system` / `_ingested_at` / `_batch_id` set documented in CLAUDE.md. Choose one (recommend a leading underscore: `_ingested_at`, `_batch_id`, `_source_system`) and reconcile code + CLAUDE.md.
- [ ] **Decide on `gene_prissy_score`.** It is the upstream simulator's column name, not a real metric. Either keep it (faithful to source, document it) or alias it in silver to a meaningful satisfaction sub-score with a lineage note. Whichever — make it a conscious, documented choice.
- [ ] **Propagate every rename through gold in the same change.** The dbt models and `_sources.yml` / `_models.yml` reference the silver names directly. Update them together; `dbt parse` must pass.
- **Acceptance:** silver column names are consistent (one prefix style, spelled out), gold models updated, `dbt parse --target glue_spark` clean, CLAUDE.md silver/metadata sections match the code.

## Milestone 1 — Close the medallion chain (make it run end-to-end)

The single highest-credibility milestone: "this actually runs," not "these are parts."

- [ ] **Stand up a Spark Thrift Server** in the Docker stack on `spark-cluster-net`, reachable at `spark-thrift:10000` with `spark.sql.defaultCatalog=glue_catalog`. The gold dbt `glue_spark` profile and both gold DAGs already target it; nothing serves it yet.
- [ ] **Build `vf_silver_dag`.** `SparkSubmitOperator` runs `process_silver.py` then `silver_dim_jobs.py`, and on success emits `Asset("vital_fold://silver/facts")` so the gold DAGs fire automatically.
- [ ] **Build `vf_populate_dag`** that drives the VitalFold Engine API (static → dynamic → DynamoDB sync → verify counts) per [docs/airflow-integration.md](../../airflow-integration.md), OR document the engine as an explicit external prerequisite in setup.
- [ ] **Run the full chain once** (populate → bronze → silver → gold) and capture the run.
- **Acceptance:** a single trigger produces gold Iceberg tables in `vital_fold_gold`, queryable via Athena; the Asset wiring is observable in the Airflow UI.

## Milestone 2 — Tests & data quality

- [ ] **Spark transform unit tests** for the risk-bearing logic: dedup keeps the right row, BP parsing, outlier flagging, late-arrival derivation, null preservation. Use local Spark or DuckDB with fixture inputs and exact expected outputs.
- [ ] **Expand dbt tests** beyond the current `unique` / `not_null` — add `relationships` across fact→dim and `accepted_values` on status/flag columns.
- [ ] **`data_quality_report` model** — per-run counts/rates for outliers, null vitals, duplicate SSN/email/policy, late arrivals, contradictions, and the appointment-status split. This is the observability story.
- [ ] **Wire pytest into CI** (the lint workflow already runs ruff/yamllint/dbt parse; add a pytest job).
- **Acceptance:** `pytest` green in CI; `dbt build` passes models + tests; the quality report populates.

## Milestone 3 — Gold breadth

Extend the proven dbt-spark + Iceberg pattern to the documented analytics surface.

- [ ] Finance: `provider_rvu_productivity`, `revenue_by_payer`, `clinic_financial_performance`.
- [ ] Operations: `clinic_daily_metrics`, `provider_workload`, `clinic_monthly_summary`.
- [ ] Clinical / satisfaction: `patient_risk_profile`, `patient_cohort_analysis`, `patient_satisfaction_trends`, `insurance_plan_metrics`.
- [ ] Each ships with a header comment (business question, grain, sources), tests, and a `_models.yml` entry.
- **Acceptance:** models build and test clean; each answers a named business question from the README's Business Outcomes table.

## Milestone 4 — Infrastructure as code

- [ ] **Terraform** for the AWS surface actually used: S3 buckets, Glue Catalog databases (`vital_fold_silver`, `vital_fold_gold`), IAM roles. Keep it minimal and real.
- [ ] If Terraform is not going to be built, **remove the IaC claim** from README/CLAUDE.md so the docs match reality.
- **Acceptance:** `terraform validate` + `terraform plan` clean, or the claim is removed everywhere.

## Milestone 5 — Serving & dashboards

- [ ] Build at least one dashboard (Superset or Athena-backed) over the gold tables.
- [ ] Capture screenshots and embed them near the top of the README.
- **Acceptance:** README shows a real dashboard image, not a "planned" placeholder.

## Milestone 6 — Documentation & repo polish

- [ ] **Reconcile referenced-but-absent docs.** CLAUDE.md's Repo Structure lists `docs/architecture.md`, `docs/data_dictionary.md`, `docs/setup_guide.md`, `docs/runbook.md`, `docs/cost_log.md`, `docs/schemas/` — none exist. Create the high-value ones (architecture, data_dictionary, setup_guide) and drop the rest from the listing.
- [ ] **Render an architecture diagram image** from the ASCII diagram for the README.
- [ ] **GitHub repo metadata** — description, topics/tags, pin the repo.
- [ ] **Refresh the stale Redshift framing** still in CLAUDE.md's "Key Design Decisions", "Cost Estimate", and "2026 Architecture Review Notes" to match the dbt-spark + Iceberg-gold architecture.
- **Acceptance:** every internal doc link resolves; no doc describes the abandoned Redshift gold path.

---

## Suggested order & rationale

`Milestone 0` (naming) → `1` (make it run) → `2` (tests) are the spine: normalize before
you build more on the names, then prove the pipeline runs, then prove it's correct. `3`–`6`
(breadth, IaC, dashboards, docs) are parallelizable polish once the spine is solid. If time
is tight, `0` + `1` alone convert the project from "impressive parts" to "a working pipeline,"
which is the biggest single jump in recruiter signal.

---

## Column Naming Verdict (silver & gold)

**Short answer: the gold layer is already production-grade; the silver layer needs a small,
targeted normalization pass, not a wholesale rename.**

**Gold (dbt models):** clean and conformed. The models already alias source-prefixed silver
columns to clean business names (`appointment_clinic_id AS clinic_id`), and aggregate columns
read well (`avg_experience_score`, `avg_wait_time_minutes`, `num_surveys`). No structural
rename needed. The one carry-through is `gene_prissy_score` (a simulator name) — address it in
silver per Milestone 0 and it flows up cleanly.

**Silver (Spark processors):** the design is sound — source-prefixing foreign keys so
downstream joins are unambiguous is the right call. But the *execution is inconsistent*, and
that inconsistency is the only thing standing between it and production-grade:

| Table | Prefix style | Verdict |
|---|---|---|
| `appointment` | `appointment_` (spelled out) | Good — keep |
| `appointment_cpt` | `cpt_` (domain acronym) | Acceptable — CPT is a recognized acronym |
| `provider`, `clinic` | `provider_`, `clinic_` | Good — keep |
| `patient_visit` | `pv_` (casual abbreviation) | **Rename** — violates the repo's own naming manual; a reader must decode "pv" |
| `dim_dates` | spelled-out date columns | Good — exemplary |

The fix is the four bullets in Milestone 0: standardize `pv_` → `visit_`, rename
`pv_visit_appointment_id_sk` → `visit_sk`, settle one audit-column convention, and make a
conscious call on `gene_prissy_score`. Because gold models and the dbt `_sources.yml`
reference these names directly, the rename is a coordinated breaking change across silver +
gold — which is exactly why it is sequenced as Milestone 0 (cheapest now) rather than a
drive-by edit.
