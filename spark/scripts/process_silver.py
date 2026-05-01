"""
Silver-layer processor (refactored).

Pure transforms produce DataFrames; a registry + publish helper handle the
side-effecting ensure-table + MERGE step. Adding a new silver table is a
single entry in SILVER_TABLES.
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from typing import Callable, Final, TypedDict

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.types import (
    StringType, IntegerType, DateType, DecimalType,
    TimestampType, DoubleType, BooleanType,
)
from pyspark.sql.functions import (
    col, current_timestamp, row_number,
    lit, coalesce, concat,
)
from pyspark.sql.window import Window

from utils.spark_file_tools import get_bronze_path


# ==============================================================================
# Constants and configuration
# ==============================================================================
BRONZE_BUCKET: Final[str] = "vital-fold-bronze-bucket-v1"
BRONZE_PREFIX: Final[str] = "bronze"

SILVER_CATALOG: Final[str] = "glue_catalog"
SILVER_NAMESPACE: Final[str] = "vital_fold_silver"

WRITE_STRATEGY: Final[str] = "copy-on-write"
ICEBERG_VERSION: Final[str] = "2"

ICEBERG_TABLE_PROPERTIES: Final[str] = f"""
    'format-version' = {ICEBERG_VERSION},
    'write.delete.mode' = '{WRITE_STRATEGY}',
    'write.update.mode' = '{WRITE_STRATEGY}',
    'write.merge.mode' = '{WRITE_STRATEGY}',
    'write.target-file-size-bytes' = '268435456'  -- 256 MiB
"""


# =============================================================================
# Logging setup
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("process_silver")


# ==============================================================================
# Helper functions
# ==============================================================================
def bronze_path(entity: str, ds: str) -> str:
    return get_bronze_path(
        entity=entity,
        ds=ds,
        bronze_bucket=BRONZE_BUCKET,
        bronze_prefix=BRONZE_PREFIX,
    )


def deduplicate_silver_df(
    df: SparkDataFrame,
    id_col: str,
    timestamp_col: str = "ingestion_timestamp",
) -> SparkDataFrame:
    window = Window.partitionBy(id_col).orderBy(col(timestamp_col).desc())
    return (
        df
        .withColumn("row_num", row_number().over(window))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )


def ensure_silver_table_exists(
    spark: SparkSession,
    df: SparkDataFrame,
    entity: str,
    catalog_name: str = SILVER_CATALOG,
    namespace: str = SILVER_NAMESPACE,
) -> None:
    """Create the Iceberg target table from `df`'s schema if it doesn't exist."""
    fq_table = f"{catalog_name}.{namespace}.{entity}"
    if spark.catalog.tableExists(fq_table):
        log.info("Silver table %s already exists", fq_table)
        return

    log.info("Creating silver table %s", fq_table)

    # Create an empty version of the DataFrame with the correct schema, to avoid
    source_view = "iv_temp_data"
    df.createOrReplaceTempView(source_view)
    try:
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {fq_table}
            USING iceberg
            TBLPROPERTIES (
                {ICEBERG_TABLE_PROPERTIES}
            )
            AS SELECT * FROM {source_view}
        """)
        spark.catalog.clearCache()  # Clear cached metadata to ensure new table is visible
    finally:
        spark.catalog.dropTempView(source_view)


def upsert_silver_df(
    spark: SparkSession,
    df: SparkDataFrame,
    entity: str,
    merge_keys: list[str],
    catalog_name: str = SILVER_CATALOG,
    namespace: str = SILVER_NAMESPACE,
) -> None:
    """MERGE `df` into the existing silver target table."""
    if not merge_keys:
        raise ValueError("upsert_silver_df requires at least one merge key")

    fq_table = f"{catalog_name}.{namespace}.{entity}"
    source_view = "iv_temp_data"
    df.createOrReplaceTempView(source_view)

    on_clause = " AND ".join(f"Target.{k} = Source.{k}" for k in merge_keys)
    merge_sql = f"""
        MERGE INTO {fq_table} AS Target
        USING {source_view} AS Source
        ON {on_clause}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """

    log.info("Upserting into %s on keys=%s", fq_table, merge_keys)
    try:
        spark.sql(merge_sql)
        spark.catalog.clearCache()  # Clear cached metadata to ensure subsequent operations see the updated table
    finally:
        spark.catalog.dropTempView(source_view)
        spark.catalog.clearCache()  # Clear cache again to ensure temp view removal is reflected


def publish_to_silver(
    spark: SparkSession,
    df: SparkDataFrame,
    entity: str,
    merge_keys: list[str],
) -> None:
    """Ensure target table exists, then MERGE `df` into it."""
    ensure_silver_table_exists(spark, df=df, entity=entity)
    upsert_silver_df(spark, df=df, entity=entity, merge_keys=merge_keys)
    log.info("Published silver.%s", entity)


def drop_bronze_meta_columns(
    df: SparkDataFrame,
    meta_columns: list[str] = ["operation_type", "dag_version_id", "run_type"],
) -> SparkDataFrame:
    return (
        df.drop(*meta_columns)
    )


# ==============================================================================
# Entity-specific processing functions (pure: bronze read → DataFrame)
# ==============================================================================
def process_appointment_df(
    spark: SparkSession,
    ds: str,
    entity: str = "appointment",
) -> SparkDataFrame:
    file_path = bronze_path(entity, ds)
    return (
        spark
        .read.parquet(file_path)
        .withColumnsRenamed({
            "provider_id":                  "appointment_provider_id",
            "clinic_id":                    "appointment_clinic_id",
            "patient_id":                   "appointment_patient_id",
            "status":                       "appointment_status",
        })
        .withColumns({
            "ingestion_timestamp":          current_timestamp(),
            "appointment_id":               col("appointment_id").cast(StringType()),
            "appointment_patient_id":       col("appointment_patient_id").cast(StringType()),
            "appointment_provider_id":      col("appointment_provider_id").cast(IntegerType()),
            "appointment_clinic_id":        col("appointment_clinic_id").cast(IntegerType()),
            "appointment_datetime":         col("appointment_datetime").cast(TimestampType()),
            "reason_for_visit":             coalesce(col("reason_for_visit"), lit("unknown")).cast(StringType()),
            "appointment_status":           coalesce(col("appointment_status"), lit("unknown")).cast(StringType()),
        })
        .transform(lambda sdf: deduplicate_silver_df(sdf, id_col="appointment_id"))
        .transform(drop_bronze_meta_columns)
    )


# ======================
# function
# ======================
def process_cpt_appointment(
    spark: SparkSession,
    ds: str,
    entity: str = "appointment_cpt",
) -> SparkDataFrame:
    file_path = bronze_path(entity, ds)
    return (
        spark.read.parquet(file_path)
        .withColumnsRenamed({
            "clinic_id":                    "cpt_clinic_id",
            "provider_id":                  "cpt_provider_id",
            "service_date":                 "cpt_service_date",
            "units":                        "cpt_units",
            "expected_amount":              "cpt_expected_amount",
        })
        .withColumns({
            "ingestion_timestamp":          current_timestamp(),
            "appointment_cpt_id":           col("appointment_cpt_id").cast(StringType()),
            "appointment_id":               col("appointment_id").cast(StringType()),
            "cpt_code_id":                  col("cpt_code_id").cast(IntegerType()),
            "cpt_provider_id":              col("cpt_provider_id").cast(IntegerType()),
            "cpt_clinic_id":                col("cpt_clinic_id").cast(IntegerType()),
            "cpt_service_date":             col("cpt_service_date").cast(DateType()),
            "cpt_units":                    coalesce(col("cpt_units"), lit(0)).cast(IntegerType()),
            "work_rvu_snapshot":            coalesce(col("work_rvu_snapshot"), lit(0.0)).cast(DecimalType(10, 4)),
            "pe_rvu_snapshot":              coalesce(col("pe_rvu_snapshot"), lit(0.0)).cast(DecimalType(10, 4)),
            "mp_rvu_snapshot":              coalesce(col("mp_rvu_snapshot"), lit(0.0)).cast(DecimalType(10, 4)),
            "total_rvu_snapshot":           coalesce(col("total_rvu_snapshot"), lit(0.0)).cast(DecimalType(10, 4)),
            "conversion_factor":            coalesce(col("conversion_factor"), lit(0.0)).cast(DecimalType(10, 4)),
            "cpt_expected_amount":          coalesce(col("cpt_expected_amount"), lit(0.0)).cast(DecimalType(10, 4)),
        })
        .drop("creation_time", "modifier_1", "modifier_2")
        .transform(lambda sdf: deduplicate_silver_df(sdf, id_col="appointment_cpt_id"))
        .transform(drop_bronze_meta_columns)
    )


# ======================
# function
# ======================
def process_provider_df(
    spark: SparkSession,
    ds: str,
    entity: str = "provider",
) -> SparkDataFrame:
    file_path = bronze_path(entity, ds)
    return (
        spark
        .read.parquet(file_path)
        .withColumnsRenamed({
            "first_name":                   "provider_first_name",
            "last_name":                    "provider_last_name",
            "email":                        "provider_email",
            "phone_number":                 "provider_phone_number",
        })
        .withColumns({
            "ingestion_timestamp":          current_timestamp(),
            "provider_id":                  col("provider_id").cast(IntegerType()),
            "provider_first_name":          coalesce(col("provider_first_name"), lit("Jamie")).cast(StringType()),
            "provider_last_name":           coalesce(col("provider_last_name"), lit("Smith")).cast(StringType()),
            "provider_email":               coalesce(col("provider_email"), lit("not_available@example.org")).cast(StringType()),
            "provider_phone_number":        coalesce(col("provider_phone_number"), lit("no_number")).cast(StringType()),
        })
        .transform(lambda sdf: deduplicate_silver_df(sdf, id_col="provider_id"))
        .transform(drop_bronze_meta_columns)
    )


# ======================
# function
# ======================
def process_patient_visit_df(
    spark: SparkSession,
    ds: str,
    entity: str = "patient_visit",
) -> SparkDataFrame:
    file_path = bronze_path(entity, ds)
    return (
        spark
        .read.parquet(file_path)
        .withColumnsRenamed({
            "appointment_id":               "pv_appointment_id",
            "patient_id":                   "pv_patient_id",
            "clinic_id":                    "pv_clinic_id",
            "provider_id":                  "pv_provider_id",
        })
        .withColumns({
            "ingestion_timestamp":          current_timestamp(),
            "checkin_time":                 coalesce(col("checkin_time"), col("creation_time")).cast(TimestampType()),
            "checkout_time":                coalesce(col("checkout_time"), col("creation_time")).cast(TimestampType()),
            "provider_seen_time":           coalesce(col("provider_seen_time"), col("creation_time")).cast(TimestampType()),
            "ekg_usage":                    coalesce(col("ekg_usage"), lit(False)).cast(BooleanType()),
            "estimated_copay":              coalesce(col("estimated_copay"), lit(0.0)).cast(DoubleType()),
            "pv_visit_appointment_id_sk":   concat(col("pv_patient_id"), lit("_"), col("pv_appointment_id")).cast(StringType()),
        })
        .transform(lambda sdf: deduplicate_silver_df(sdf, id_col="pv_visit_appointment_id_sk"))
        .transform(drop_bronze_meta_columns)
    )


# ======================
# function
# ======================
def process_clinic_df(
    spark: SparkSession,
    ds: str,
    entity: str = "clinic",
) -> SparkDataFrame:
    # No coalesce: clinic_id and region must be present for downstream joins;
    # rows missing either should be dropped, not defaulted.
    file_path = bronze_path(entity, ds)
    return (
        spark
        .read.parquet(file_path)
        .withColumnsRenamed({"region": "clinic_region"})
        .withColumn("ingestion_timestamp", current_timestamp())
        .transform(lambda sdf: deduplicate_silver_df(sdf, id_col="clinic_id"))
        .transform(drop_bronze_meta_columns)
    )


# ======================
# function
# ======================
def process_survey_df(
    spark: SparkSession,
    ds: str,
    entity: str = "survey",
) -> SparkDataFrame:
    file_path = bronze_path(entity, ds)
    return (
        spark
        .read.parquet(file_path)
        .withColumns({
            "ingestion_timestamp":          current_timestamp(),
            "gene_prissy_score":            coalesce(col("gene_prissy_score"), lit(0)).cast(IntegerType()),
            "experience_score":             coalesce(col("experience_score"), lit(0)).cast(IntegerType()),
            "feedback_comments":            coalesce(col("feedback_comments"), lit("no_comment")).cast(StringType()),
        })
        .transform(lambda sdf: deduplicate_silver_df(sdf, id_col="survey_id"))
        .transform(drop_bronze_meta_columns)
    )


# ==============================================================================
# Silver pipeline registry — single source of truth
# ==============================================================================
ProcessFn = Callable[[SparkSession, str, str], SparkDataFrame]


class SilverTableSpec(TypedDict):
    process: ProcessFn
    merge_keys: list[str]


SILVER_TABLES: Final[dict[str, SilverTableSpec]] = {
    "appointment":      {"process": process_appointment_df,         "merge_keys": ["appointment_id"]},
    "appointment_cpt":  {"process": process_cpt_appointment,        "merge_keys": ["appointment_cpt_id"]},
    "provider":         {"process": process_provider_df,            "merge_keys": ["provider_id"]},
    "patient_visit":    {"process": process_patient_visit_df,       "merge_keys": ["pv_visit_appointment_id_sk"]},
    "clinic":           {"process": process_clinic_df,              "merge_keys": ["clinic_id"]},
    "survey":           {"process": process_survey_df,              "merge_keys": ["survey_id"]},
}


def run_silver_entity(spark: SparkSession, entity: str, ds: str) -> None:
    """Process one entity end-to-end: bronze → transform → silver upsert."""
    if entity not in SILVER_TABLES:
        raise KeyError(f"Unknown silver entity '{entity}'. Known: {list(SILVER_TABLES)}")
    
    
    spec = SILVER_TABLES[entity]
    log.info("Running silver pipeline for entity=%s ds=%s", entity, ds)
    
    df = spec["process"](spark, ds=ds, entity=entity)
    publish_to_silver(spark, df=df, entity=entity, merge_keys=spec["merge_keys"])


# ==============================================================================
# Entrypoint
# ==============================================================================
def main() -> None:
    log.info("argv=%s", sys.argv)

    parser = argparse.ArgumentParser(description="Run silver-layer processing.")
    parser.add_argument("--ds", required=True, help="Logical date (YYYY-MM-DD)")
    parser.add_argument(
        "--entities",
        default="",
        help="Space-separated list of silver entities to process. Empty = all.",
    )
    args = parser.parse_args()

    ds = args.ds
    entities = args.entities.split() or list(SILVER_TABLES)

    log.info("Starting silver processing for ds=%s entities=%s \n", ds, entities)
    log.info("Creating SparkSession...\n")


    spark = (
        SparkSession.builder
        .appName("process-silver")
        .config("spark.sql.catalog.glue_alt", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.glue_alt.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config("spark.sql.catalog.glue_alt.warehouse", "s3://vital-fold-spark-iceberg-glue-v1/")
        .config("spark.sql.catalog.glue_alt.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    log.info(
        "SparkSession ready: app_id=%s, master=%s, version=%s",
        spark.sparkContext.applicationId,
        spark.sparkContext.master,
        spark.version,
    )

    try:
        t_start = time.monotonic()

        # Run each entity sequentially to avoid overwhelming the cluster with multiple
        for entity in entities:
            run_silver_entity(spark, entity=entity, ds=ds)
            spark.catalog.clearCache()  # Clear cached metadata to ensure subsequent operations see the updated table
        
        log.info("Silver run completed in %.2fs", time.monotonic() - t_start)
    except Exception:
        log.exception("Silver run failed")
        raise
    finally:
        log.info("Stopping SparkSession")
        spark.stop()
        log.info("Done")


if __name__ == "__main__":
    main()
