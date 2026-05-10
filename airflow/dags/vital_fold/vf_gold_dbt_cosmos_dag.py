"""
vf_gold_dbt_cosmos_dag.py — Cosmos-rendered version of vf_gold_dbt_pipeline.

Replaces the three-task BashOperator pipeline in vf_gold_dbt_dag.py with
astronomer-cosmos's DbtTaskGroup, which renders each dbt model (and its
tests) as its own Airflow task. Same Asset trigger, same dbt project,
same `glue_spark` profile — only the internal task structure differs.

Phase 1 deploy: starts paused (`is_paused_upon_creation=True`). Run parity
testing manually against the live `vf_gold_dbt_pipeline` before un-pausing.
Full migration plan: ~/.claude/plans/cosmos-migration.md.

Prerequisites (one-time, BEFORE this DAG file is loaded by the scheduler):

1. Airflow image rebuilt so `astronomer-cosmos>=1.14.0` is installed:
       docker compose build airflow-worker airflow-scheduler airflow-dag-processor
       docker compose up -d

2. `dbt deps` and `dbt parse` run from inside the worker so the manifest
   exists at `/opt/airflow/dbt/target/manifest.json` AND `dbt_utils` is
   installed in `dbt_packages/`. Cosmos uses LoadMode.DBT_MANIFEST so it
   never hits Spark Thrift at DAG parse time:

       docker compose exec airflow-worker bash -lc '
         cd /opt/airflow/dbt &&
         dbt deps  --profiles-dir /opt/airflow/dbt &&
         dbt parse --profiles-dir /opt/airflow/dbt --target glue_spark
       '

3. Spark Thrift Server reachable at `spark-thrift:10000` for actual model
   execution (NOT for DAG parse — see point 2). See
   docs/spark_thrift_handoff.md.
"""

from __future__ import annotations
from datetime import timedelta

import pendulum
from airflow.sdk import dag, Asset

from cosmos import (
    DbtTaskGroup,
    ProjectConfig,
    ProfileConfig,
    ExecutionConfig,
    RenderConfig,
    LoadMode,
)


# Logical Asset name — same constant defined in dags/spark_submit_dag.py.
# Airflow uses this opaque identifier to wire silver producer (process_silver
# task) to this gold consumer.
SILVER_FACTS_ASSET = Asset("vital_fold://silver/facts")

DBT_PROJECT_DIR = "/opt/airflow/dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt"
DBT_MANIFEST_PATH = f"{DBT_PROJECT_DIR}/target/manifest.json"


project_config = ProjectConfig(
    dbt_project_path=DBT_PROJECT_DIR,
    manifest_path=DBT_MANIFEST_PATH,
)

profile_config = ProfileConfig(
    # VitalFold-only dbt project — single profile, single target.
    profile_name="vitalfold",
    target_name="glue_spark",
    profiles_yml_filepath=f"{DBT_PROFILES_DIR}/profiles.yml",
)

execution_config = ExecutionConfig(
    # `uv pip install` lands dbt at /usr/local/bin/dbt in the Airflow image.
    dbt_executable_path="/usr/local/bin/dbt",
)

render_config = RenderConfig(
    # Pre-built manifest avoids hitting Spark Thrift at DAG parse time —
    # the scheduler stays healthy even if the cluster is down.
    load_method=LoadMode.DBT_MANIFEST,
)


default_args = {
    "owner": "vital_fold",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}


@dag(
    dag_id="vf_gold_dbt_cosmos_pipeline",
    description="Cosmos-rendered VitalFold gold dbt models. Replaces vf_gold_dbt_pipeline at Phase 3 cutover.",
    schedule=[SILVER_FACTS_ASSET],
    start_date=pendulum.datetime(2026, 4, 1, tz="US/Eastern"),
    catchup=False,
    is_paused_upon_creation=True,
    default_args=default_args,
    tags=["dbt", "spark", "iceberg", "vital-fold", "gold", "cosmos"],
)
def vf_gold_dbt_cosmos_pipeline():
    DbtTaskGroup(
        group_id="vital_fold_models",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=render_config,
    )


vf_gold_dbt_cosmos_pipeline()
