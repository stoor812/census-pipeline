"""
transform.py — Census pipeline silver layer transformation
Reads raw bronze parquet from S3, applies cleaning and renaming,
writes clean silver parquet back to S3.
"""

import io
import logging
import os
from datetime import datetime

import boto3
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# --- Constants -----------------------------------------------------------

COLUMN_RENAME = {
    "CENSUS_YEAR": "census_year",
    "DGUID": "dguid",
    "ALT_GEO_CODE": "alt_geo_code",
    "GEO_LEVEL": "geo_level",
    "GEO_NAME": "geo_name",
    "DATA_QUALITY_FLAG": "data_quality_flag",
    "CHARACTERISTIC_ID": "characteristic_id",
    "CHARACTERISTIC_NAME": "characteristic_name",
    "CHARACTERISTIC_NOTE": "characteristic_note",
    "C1_COUNT_TOTAL": "count_total",
    "C2_COUNT_MEN+": "count_men",
    "C3_COUNT_WOMEN+": "count_women",
    "C10_RATE_TOTAL": "rate_total",
    "C11_RATE_MEN+": "rate_men",
    "C12_RATE_WOMEN+": "rate_women",
}

COLUMNS_TO_DROP = [
    "TNR_SF", "TNR_LF",
    "SYMBOL", "SYMBOL.1", "SYMBOL.2",
    "SYMBOL.3", "SYMBOL.4", "SYMBOL.5",
]

FLOAT_COLUMNS = [
    "count_total", "count_men", "count_women",
    "rate_total", "rate_men", "rate_women",
]


# --- Validation ----------------------------------------------------------

def _validate_env():
    """Raise early if any required environment variables are missing."""
    required = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_REGION", "S3_BUCKET_NAME"]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        raise ValueError(f"Missing required environment variables: {missing}")


# --- S3 client -----------------------------------------------------------

def _get_s3_client():
    """Build a boto3 S3 client from environment variables."""
    return boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION", "us-east-1"),
    )


# --- Step 1: Read bronze from S3 -----------------------------------------

def read_bronze(s3_key: str) -> pd.DataFrame:
    """
    Read a parquet file from the S3 bronze layer into a DataFrame.

    Args:
        s3_key: S3 key of the bronze parquet file (e.g. bronze/census_fed_2013_20240101_120000.parquet).

    Returns:
        Raw bronze DataFrame exactly as written by ingest.py.

    Raises:
        boto3 ClientError if the key doesn't exist or credentials are invalid.
    """
    bucket = os.getenv("S3_BUCKET_NAME")
    logger.info(f"Reading bronze parquet from s3://{bucket}/{s3_key}...")

    s3 = _get_s3_client()
    response = s3.get_object(Bucket=bucket, Key=s3_key)
    parquet_bytes = response["Body"].read()

    df = pd.read_parquet(io.BytesIO(parquet_bytes))

    logger.info(f"Bronze read complete — {len(df)} rows, {len(df.columns)} columns.")
    return df


# --- Step 2: Transform ---------------------------------------------------

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply all transformations to the raw bronze DataFrame.

    Steps:
      1. Drop unused StatCan internal columns
      2. Rename columns to clean snake_case
      3. Cast numeric value columns using pd.to_numeric (coerce invalids to NaN)
      4. Reset index

    Args:
        df: Raw bronze DataFrame from read_bronze().

    Returns:
        Cleaned silver DataFrame ready for validation and loading.

    Raises:
        ValueError: If the DataFrame is empty after transformation.
    """
    logger.info("Starting transformation...")

    # Step 1 — drop internal StatCan columns that aren't useful for analysis.
    # Only drop columns that actually exist — guards against schema changes.
    cols_to_drop = [col for col in COLUMNS_TO_DROP if col in df.columns]
    df = df.drop(columns=cols_to_drop)
    logger.info(f"Dropped {len(cols_to_drop)} columns: {cols_to_drop}")

    # Step 2 — rename to clean snake_case using the mapping.
    # Only rename columns that exist in the DataFrame — same guard as above.
    rename_map = {k: v for k, v in COLUMN_RENAME.items() if k in df.columns}
    if not rename_map:
        logger.warning("No expected columns found in DataFrame — schema may have changed.")
    df = df.rename(columns=rename_map)
    logger.info(f"Renamed {len(rename_map)} columns.")

    # Step 3 — cast value columns to float.
    # pd.to_numeric with errors="coerce" converts any non-numeric value
    # (StatCan symbols like "F", "x", "...") directly to NaN in one step.
    cast_float_cols = [col for col in FLOAT_COLUMNS if col in df.columns]
    for col in cast_float_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    logger.info(f"Cast {len(cast_float_cols)} columns to float.")

    # Cast characteristic_id to nullable integer.
    # Int64 (capital I) is used instead of int64 because it supports NaN —
    # regular int64 would crash if any values are malformed.
    if "characteristic_id" in df.columns:
        df["characteristic_id"] = pd.to_numeric(df["characteristic_id"], errors="coerce").astype("Int64")
        logger.info("Cast characteristic_id to Int64.")

    # Step 4 — reset index so it's clean after all the column operations.
    df = df.reset_index(drop=True)

    if df.empty:
        raise ValueError("DataFrame is empty after transformation — nothing to write to silver.")

    logger.info(f"Transformation complete — {len(df)} rows, {len(df.columns)} columns.")
    return df


# --- Step 3: Write silver to S3 ------------------------------------------

def save_silver(df: pd.DataFrame) -> str:
    """
    Write the cleaned DataFrame to S3 as a timestamped parquet in the silver layer.

    Args:
        df: Cleaned silver DataFrame from transform_data().

    Returns:
        The S3 key the silver parquet was written to.
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    s3_key = f"silver/census_fed_2013_{timestamp}.parquet"
    bucket = os.getenv("S3_BUCKET_NAME")

    logger.info(f"Writing {len(df)} rows to s3://{bucket}/{s3_key}...")

    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False, engine="pyarrow")
    parquet_buffer.seek(0)

    s3 = _get_s3_client()
    s3.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=parquet_buffer.getvalue(),
    )

    logger.info(f"Silver layer written: s3://{bucket}/{s3_key}")
    return s3_key


# --- Orchestrator --------------------------------------------------------

def run_transform(bronze_key: str) -> str:
    """
    Run the full transformation sequence:
      1. Read bronze parquet from S3
      2. Apply all transformations
      3. Write clean silver parquet to S3

    Args:
        bronze_key: S3 key of the bronze parquet produced by ingest.py.

    Returns:
        S3 key of the written silver parquet.
    """
    _validate_env()
    logger.info("=== Starting census transformation ===")

    df_bronze = read_bronze(bronze_key)
    df_silver = transform_data(df_bronze)
    s3_key = save_silver(df_silver)

    logger.info(f"=== Transformation complete — silver key: {s3_key} ===")
    return s3_key


# --- Entrypoint ----------------------------------------------------------

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: python -m src.pipeline.transform <bronze_s3_key>")
        sys.exit(1)
    run_transform(sys.argv[1])