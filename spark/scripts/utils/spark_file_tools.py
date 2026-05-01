from __future__ import annotations

import logging

def write_to_s3(df, path, format="parquet", mode="overwrite"):
    logging.info(f"Writing data to S3 path: {path}, format: {format}, mode: {mode}")
    df.write.format(format).mode(mode).save(path)

def read_from_s3(spark, path, format="parquet"):
    logging.info(f"Reading data from S3 path: {path}")
    return spark.read.format(format).load(path)



def get_bronze_path(
    entity: str, 
    ds: str,
    bronze_bucket: str, 
    bronze_prefix: str
) -> str:
    return f"s3a://{bronze_bucket}/{bronze_prefix}/{entity}/{ds}.parquet"

