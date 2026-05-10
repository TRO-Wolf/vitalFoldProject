"""
vf_gold_dbt_dag.py — runs dbt against the VitalFold gold-layer Iceberg models.

Triggered by the Asset emitted from spark_submit_my_example_dag's
`process_silver` task — gold builds only after a silver MERGE completes.
Tasks: dbt deps -> dbt run --select vital_fold -> dbt test --select vital_fold.

Targets the `glue_spark` profile in dbt/profiles.yml (Spark Thrift Server +
Glue Catalog Iceberg). No auth env vars — Thrift has no auth on the
private Docker network.
"""

from __future__ import annotations

import os
from datetime import timedelta

import pendulum
from airflow.sdk import dag, Asset
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator


# Logical Asset name shared with spark_submit_dag.py. Airflow uses this
# as an opaque identifier for producer/consumer wire-up — it is NOT parsed
# as a real URI, so the scheme is purely conventional.
SILVER_FACTS_ASSET = Asset("vital_fold://silver/facts")

DBT_PROJECT_DIR = "/opt/airflow/dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt"
DBT_TARGET = "glue_spark"
DBT_SELECT = "vital_fold"

DBT_BASE_CMD = (
    f"dbt --no-write-json "
    f"--project-dir {DBT_PROJECT_DIR} "
    f"--profiles-dir {DBT_PROFILES_DIR} "
    f"--target {DBT_TARGET}"
)

default_args = {
    "owner": "vital_fold",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}


@dag(
    dag_id="vf_gold_dbt_pipeline",
    description="Build VitalFold gold-layer Iceberg models via dbt-spark + Spark Thrift Server.",
    schedule=[SILVER_FACTS_ASSET],
    start_date=pendulum.datetime(2026, 4, 1, tz="US/Eastern"),
    catchup=False,
    default_args=default_args,
    tags=["dbt", "spark", "iceberg", "vital-fold", "gold"],
)
def vf_gold_dbt_pipeline():

    start = EmptyOperator(task_id="start")

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"{DBT_BASE_CMD} deps",
        env={**os.environ},
    )

    dbt_run = BashOperator(
        task_id="dbt_run_vital_fold",
        bash_command=f"{DBT_BASE_CMD} run --select {DBT_SELECT}",
        env={**os.environ},
    )

    dbt_test = BashOperator(
        task_id="dbt_test_vital_fold",
        bash_command=f"{DBT_BASE_CMD} test --select {DBT_SELECT}",
        env={**os.environ},
    )

    end = EmptyOperator(task_id="end")

    start >> dbt_deps >> dbt_run >> dbt_test >> end


vf_gold_dbt_pipeline()
