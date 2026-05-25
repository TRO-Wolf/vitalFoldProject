# Portfolio Readiness Gaps

Working document of everything still between the current repo state and "polished public portfolio." Grouped by priority. Check off as items land.

## 🔴 Honesty gaps — must fix

The repo currently claims things that aren't true. A recruiter reading the README will be told one architecture and see another in code.

- [ ] **1. README is stale.** Describes the abandoned dbt-redshift / Spectrum / Redshift Serverless gold architecture across ~10 sections (tagline, "What this demonstrates" bullets, architecture diagram, Tech Stack table, Gold layer description, pipeline diagram, Project Structure, Quick Start, Design Decisions, Cost). Needs rewrite to dbt-spark + Spark Thrift Server + Iceberg gold.
- [ ] **2. README references files that don't exist.** Top-level `docker-compose.yml`, `pyproject.toml`, `Makefile`, `terraform/modules/*`, six `docs/*.md` files (`architecture.md`, `data_dictionary.md`, `setup_guide.md`, `runbook.md`, `cost_log.md`, `schemas/`), and `scripts/seed_connections.sh`. Either build them or strip the references.
- [ ] **3. Cost table is fiction.** Numbers based on Redshift Serverless pricing that no longer applies. Either rebuild for the dbt-spark stack or remove until real costs are tracked.

## 🟠 Runs-end-to-end gaps — code exists but pipeline is wired to ghosts

- [ ] **4. No Spark Thrift Server in Docker stack.** Both gold DAGs and dbt's `glue_spark` profile target `spark-thrift:10000` on `spark-cluster-net`, but nothing in the repo starts a Thrift Server. DAGs parse cleanly but fail at execution. Either add a Thrift Server service to compose, or scope-cut by removing the gold layer from the portfolio narrative.
- [ ] **5. No silver-producer DAG.** Gold DAGs trigger on `Asset("vital_fold://silver/facts")` which is never emitted. Bronze runs, gold sits idle — the medallion chain is broken. Need a `vf_silver_dag.py` that runs `process_silver.py` + `silver_dim_jobs.py` via SparkSubmit and emits the asset.
- [ ] **6. No populate DAG.** Bronze extracts from Aurora DSQL, but nothing in this repo populates Aurora. Either add a `vf_populate_dag.py` calling the VitalFold Engine API, or document the engine as a documented external prerequisite in setup.

## 🟡 Professional-rigor signals — visible to recruiters reviewing the repo

- [ ] **7. No tests.** dbt has inline tests (good). `spark/tests/` is empty. No Python unit tests anywhere. One test per layer would change the picture.
- [ ] **8. No CI.** `.github/workflows/` is empty. Even a 20-line lint workflow (`ruff check`, `dbt parse`, `yamllint`) signals professional rigor.
- [ ] **9. No LICENSE.** Public repo should have one. MIT or Apache 2.0 are standard for portfolio repos.
- [ ] **10. No `terraform/` content** despite Terraform being claimed in Tech Stack. The `terraform/{environments,modules}/` dirs exist but are empty. Either build a minimal module (S3 + Glue catalog + IAM is ~80 lines) or drop the Terraform claim.

## 🟢 Polish — last 10%

- [ ] **11. Empty placeholder dirs.** `spark/{config,jobs/{bronze,silver,gold},lib,tests}/`, `terraform/{environments,modules}/`, `scripts/`. Either populate or remove from the tracked tree.
- [ ] **12. `Prompt.md` at repo root.** Original brief from before the repo existed. Historical artifact, not work product. Delete.
- [ ] **13. No dashboard or architecture image.** Even if Superset dashboards aren't built yet, a rendered architecture diagram (PNG from the ASCII version) makes the README scannable. Adds significant first-impression value.
- [ ] **14. GitHub repo metadata.** Description, topics/tags, pinned status — set from the GitHub UI, not from code.

## Suggested execution order

In descending leverage-per-effort:

1. **Rewrite README** to match current dbt-spark + Iceberg-gold reality (highest visibility, smallest scope)
2. **Add LICENSE + delete `Prompt.md` + clean up empty scaffolding** (10 min of housekeeping)
3. **Add basic CI workflow** (`.github/workflows/lint.yml` — ruff + yamllint + dbt parse)
4. **Build the silver DAG** to close the medallion chain (biggest "this actually runs" credibility win)
5. **Decide on Spark Thrift Server**: add to compose, or scope-cut by removing the gold layer entirely (still a complete bronze+silver story)
6. **Skinny Terraform module** if you want to claim it
7. **Architecture diagram image** for README
