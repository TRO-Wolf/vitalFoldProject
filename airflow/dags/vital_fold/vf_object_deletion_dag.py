from __future__ import annotations
from airflow.sdk import dag, task, Param
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pendulum
import logging


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


BUCKET = "vital-fold-bronze-bucket-v1"

default_args = {
    'owner': 'vital_fold',
    'retries': 1,
    'retry_delay': 2,
}


@dag(
    dag_id="vf_object_deletion_dag",
    schedule=None,
    start_date=pendulum.datetime(2026, 4, 1, tz="US/Eastern"),
    catchup=False,
    default_args=default_args,
    tags=["vital-fold", "maintenance"],
    params={
        "prefix": Param("", type="string", description="Optional key prefix to scope the delete (empty = whole bucket)."),
    },
    doc_md=f"""
    ### vf_object_deletion_dag
    Deletes **all objects** in `s3://{BUCKET}` while leaving the bucket itself in place.

    - Manually triggered (`schedule=None`).
    - Optional `prefix` param to scope the delete (e.g. `bronze/appointment/`).
    - Batches deletes in groups of 1000 (S3 DeleteObjects API limit).
    """,
)
def vf_object_deletion_dag():

    @task
    def delete_all_objects(**context) -> int:
        prefix = context["params"].get("prefix") or ""
        hook = S3Hook(aws_conn_id="vital_fold_aws")

        keys = hook.list_keys(bucket_name=BUCKET, prefix=prefix) or []
        if not keys:
            logger.info("No objects found in s3://%s/%s — nothing to delete.", BUCKET, prefix)
            return 0

        logger.info("Deleting %d objects from s3://%s/%s", len(keys), BUCKET, prefix)
        for i in range(0, len(keys), 1000):
            batch = keys[i : i + 1000]
            hook.delete_objects(bucket=BUCKET, keys=batch)
            logger.info("Deleted batch %d-%d (%d keys)", i, i + len(batch) - 1, len(batch))

        return len(keys)

    delete_all_objects()


vf_object_deletion_dag()
