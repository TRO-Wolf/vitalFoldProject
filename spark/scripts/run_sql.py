"""
Generic Spark SQL runner.

Submitted via Airflow's SparkSubmitOperator. Takes a single positional
argument: the SQL query to execute against the active Spark catalog.

Usage (locally, for testing):
    spark-submit run_sql.py "SELECT * FROM my_catalog.my_db.my_table LIMIT 10"
"""

from __future__ import annotations

import logging
import sys
import time
from functools import partial
from typing import Final

from pyspark.sql import SparkSession

from utils.spark_file_tools import get_bronze_path


BRONZE_BUCKET: Final[str] = "vital-fold-bronze-bucket-v1"
BRONZE_PREFIX: Final[str] = "bronze"




# Write INFO+ to stdout so it's captured by the Spark driver log,
# which Airflow's SparkSubmitOperator forwards into the task log.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("run_sql")


def main() -> None:
    if len(sys.argv) < 2:
        log.error("Usage: run_sql.py '<SQL query>'")
        raise SystemExit(2)

    query = sys.argv[1]
    log.info("Received query: %s", query)

    log.info("Creating SparkSession...")
    spark = (
        SparkSession.builder
        .appName("airflow-run-sql")
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config("spark.sql.catalog.glue_catalog.warehouse", "s3://vital-fold-spark-iceberg-glue-v1/")
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.glue_catalog.glue.lakeformation.enabled", "false")
        .getOrCreate()
    )

    log.info('\n------------------------------------------')
    warehouse = spark.conf.get("spark.sql.catalog.glue_catalog.warehouse")
    log.info(f"Using warehouse: {warehouse}\n")
    log.info('------------------------------------------\n')

    # Silence Spark's own INFO chatter so our logs stand out.
    # Flip to "INFO" or "DEBUG" temporarily when diagnosing Spark internals.
    spark.sparkContext.setLogLevel("WARN")

    log.info(
        "SparkSession ready: app_id=%s, master=%s, version=%s",
        spark.sparkContext.applicationId,
        spark.sparkContext.master,
        spark.version,
    )

    try:
        t_start = time.monotonic()
        log.info("Executing query...")
        df = spark.sql(query)

        row_count = df.count()
        elapsed = time.monotonic() - t_start

        log.info("Query completed in %.2fs", elapsed)
        log.info("Row count: %d", row_count)
        log.info("Schema: %s", df.schema.simpleString())

        log.info("Results:")
        df.show(n=20, truncate=False)
    except Exception:
        log.exception("Query failed")
        raise
    finally:
        log.info("Stopping SparkSession")
        spark.stop()
        log.info("Done")


if __name__ == "__main__":
    main()
