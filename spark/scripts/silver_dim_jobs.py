from __future__ import annotations

import argparse
import logging
import sys
import time
from typing import Callable, Final, TypedDict

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.types import DateType
import pyspark.sql.functions as F

from datetime import date

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
log = logging.getLogger("silver_dim_jobs")



DATE_COLUMN = 'calendar_date'



def create_silver_dim_dates(
    spark: SparkSession,
    start_date: date = date(2025, 1, 1),
    end_date: date = date(2035, 12, 31),
    date_column_name: str = DATE_COLUMN,
) -> None:
    """Create the silver dim_dates Iceberg table if it doesn't already exist."""
    fq_table = f"{SILVER_CATALOG}.{SILVER_NAMESPACE}.dim_dates"
    if spark.catalog.tableExists(fq_table):
        log.info("Silver dimension table %s already exists — skipping", fq_table)
        return

    log.info("Building silver dim_dates from %s to %s", start_date, end_date)

    silver_dim = (
        spark.sql(
            f"SELECT explode(sequence(DATE'{start_date}', DATE'{end_date}', INTERVAL 1 DAY)) "
            f"AS {date_column_name}"
        )
        .withColumns({
            "date_key":           F.date_format(date_column_name, "yyyyMMdd").cast("int"),
            "year":               F.year(date_column_name).cast("smallint"),
            "quarter":            F.quarter(date_column_name).cast("tinyint"),
            "month":              F.month(date_column_name).cast("tinyint"),
            "year_month":         F.date_format(date_column_name, "yyyyMM").cast("int"),
            "year_quarter":       F.date_format(date_column_name, "yyyy'Q'q").cast("string"),
            "month_name":         F.date_format(date_column_name, "MMMM"),
            "month_short":        F.date_format(date_column_name, "MMM"),
            "day":                F.dayofmonth(date_column_name).cast("tinyint"),
            "day_of_year":        F.dayofyear(date_column_name).cast("smallint"),
            "day_of_week_iso":    ((F.dayofweek(date_column_name) + 5) % 7 + 1).cast("tinyint"),
            "day_name":           F.date_format(date_column_name, "EEEE"),
            "day_short":          F.date_format(date_column_name, "EEE"),
            "week_of_year_iso":   F.weekofyear(date_column_name).cast("tinyint"),
            "iso_year":           F.expr(f"extract(YEAROFWEEK FROM {date_column_name})").cast("smallint"),
            "week_start_date":    F.date_trunc("week", date_column_name).cast(DateType()),
            "month_start_date":   F.trunc(date_column_name, "MM"),
            "month_end_date":     F.last_day(date_column_name),
            "quarter_start_date": F.trunc(date_column_name, "Q"),
            "quarter_end_date":   F.date_add(F.add_months(F.trunc(date_column_name, "Q"), 3), -1),
            "year_start_date":    F.trunc(date_column_name, "YEAR"),
            "year_end_date":      F.expr(f"make_date(year({date_column_name}), 12, 31)"),
            "prior_year_date":    F.add_months(date_column_name, -12),
            "prior_month_date":   F.add_months(date_column_name, -1),
            "is_weekend":         F.dayofweek(date_column_name).isin(1, 7),
            "is_month_end":       F.col(date_column_name) == F.last_day(date_column_name),
            "is_quarter_end":     F.col(date_column_name) == F.date_add(F.add_months(F.trunc(date_column_name, "Q"), 3), -1),
            "is_year_end":        F.col(date_column_name) == F.expr(f"make_date(year({date_column_name}), 12, 31)"),
        })
    )

    source_view = "iv_silver_dim_dates"
    silver_dim.createOrReplaceTempView(source_view)
    try:
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {fq_table}
            USING iceberg
            TBLPROPERTIES (
                {ICEBERG_TABLE_PROPERTIES}
            )
            AS SELECT * FROM {source_view}
        """)
        spark.catalog.clearCache()
        log.info("Created silver dimension table %s", fq_table)
    finally:
        spark.catalog.dropTempView(source_view)


def main() -> None:
    log.info("argv=%s", sys.argv)

    parser = argparse.ArgumentParser(description="Build silver dimension tables.")
    parser.add_argument(
        "--start-date",
        type=date.fromisoformat,
        default=date(2025, 1, 1),
        help="Inclusive lower bound of dim_dates range (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--end-date",
        type=date.fromisoformat,
        default=date(2035, 12, 31),
        help="Inclusive upper bound of dim_dates range (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--date-column",
        default=DATE_COLUMN,
        help=f"Name of the natural-key date column (default: {DATE_COLUMN}).",
    )
    args = parser.parse_args()

    if args.end_date < args.start_date:
        parser.error("--end-date must be on or after --start-date")

    log.info("Creating SparkSession...")
    spark = (
        SparkSession.builder
        .appName("silver-dim-jobs")
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
        create_silver_dim_dates(
            spark,
            start_date=args.start_date,
            end_date=args.end_date,
            date_column_name=args.date_column,
        )
        log.info("Silver dim run completed in %.2fs", time.monotonic() - t_start)
    except Exception:
        log.exception("Silver dim run failed")
        raise
    finally:
        log.info("Stopping SparkSession")
        spark.stop()
        log.info("Done")


if __name__ == "__main__":
    main()
