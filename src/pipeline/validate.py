"""
validate.py — Census pipeline silver layer validation
Runs critical and advisory checks on the silver DataFrame.
Critical checks raise and stop the pipeline.
Advisory checks log warnings but allow the pipeline to continue.
"""

import logging
import os

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# --- Constants -----------------------------------------------------------

VALID_GEO_LEVELS = {
    "Country",
    "Province",
    "Territory",
    "Federal electoral district (2013 Representation Order)",
}

CRITICAL_COLUMNS = [
    "dguid",
    "characteristic_id",
    "geo_name",
    "characteristic_name",
]

MAX_NULL_RATE = 0.5  # warn if more than 50% of count_total is null

CHARACTERISTIC_ID_MIN = 1
CHARACTERISTIC_ID_MAX = 2631


# --- Validation ----------------------------------------------------------

def validate(df: pd.DataFrame) -> dict:
    """
    Run all validation checks on the silver DataFrame.

    Critical checks (raise ValueError and stop the pipeline):
      1. Empty DataFrame
      2. Critical columns missing from schema entirely
      3. Null values in critical columns
      4. Duplicate composite keys (dguid + characteristic_id)

    Advisory checks (log warnings, pipeline continues):
      5. Unexpected geo_level values
      6. characteristic_id values outside expected range 1–2631
      7. count_total null rate above MAX_NULL_RATE threshold

    Args:
        df: Silver DataFrame produced by transform.py.

    Returns:
        Summary dict with keys:
          - rows: total row count
          - geo_level_warnings: list of unexpected geo_level values found
          - null_rate_warnings: list of columns where null rate exceeded threshold
          - null_rate: null rate of count_total as a float (0.0–1.0)

    Raises:
        ValueError: On any critical check failure.
    """
    logger.info("=== Starting validation ===")

    # --- Critical Check 1: Empty DataFrame -------------------------------
    if df.empty:
        raise ValueError("Validation failed — DataFrame is empty.")
    logger.info(f"Row count: {len(df)} rows. ✓")

    # --- Critical Check 2: Critical columns exist in schema --------------
    missing_columns = [col for col in CRITICAL_COLUMNS if col not in df.columns]
    if missing_columns:
        raise ValueError(
            f"Validation failed — critical columns missing from schema: {missing_columns}"
        )
    logger.info(f"All critical columns present: {CRITICAL_COLUMNS} ✓")

    # --- Critical Check 3: No nulls in critical columns ------------------
    null_counts = {col: int(df[col].isnull().sum()) for col in CRITICAL_COLUMNS}
    columns_with_nulls = {col: count for col, count in null_counts.items() if count > 0}
    if columns_with_nulls:
        raise ValueError(
            f"Validation failed — null values found in critical columns: {columns_with_nulls}"
        )
    logger.info("No nulls in critical columns. ✓")

    # --- Critical Check 4: No duplicate composite keys -------------------
    # Exclude rows where either key column is null before checking —
    # pandas treats NaN as equal in duplicated(), causing false positives.
    key_cols = ["dguid", "characteristic_id"]
    df_keyed = df.dropna(subset=key_cols)
    duplicate_count = df_keyed.duplicated(subset=key_cols).sum()
    if duplicate_count > 0:
        raise ValueError(
            f"Validation failed — {duplicate_count} duplicate dguid + characteristic_id "
            f"combinations found. Each row must be uniquely identified."
        )
    logger.info("No duplicate composite keys. ✓")

    # --- Advisory Check 5: Unexpected geo_level values -------------------
    geo_level_warnings = []
    if "geo_level" in df.columns:
        actual_geo_levels = set(df["geo_level"].dropna().unique())
        unexpected = actual_geo_levels - VALID_GEO_LEVELS
        if unexpected:
            geo_level_warnings = sorted(unexpected)
            logger.warning(
                f"Unexpected geo_level values found: {geo_level_warnings}. "
                f"Expected: {VALID_GEO_LEVELS}"
            )
        else:
            logger.info(f"All geo_level values are valid. ✓")
    else:
        logger.warning("Column 'geo_level' not found — advisory check 5 skipped.")

    # --- Advisory Check 6: characteristic_id range -----------------------
    if "characteristic_id" in df.columns:
        valid_ids = df["characteristic_id"].dropna()
        out_of_range = valid_ids[
            (valid_ids < CHARACTERISTIC_ID_MIN) | (valid_ids > CHARACTERISTIC_ID_MAX)
        ]
        if not out_of_range.empty:
            unique_bad = sorted(out_of_range.unique().tolist())
            preview = unique_bad[:20]
            truncated = f" (showing first 20 of {len(unique_bad)})" if len(unique_bad) > 20 else ""
            logger.warning(
                f"{len(out_of_range)} characteristic_id values outside expected range "
                f"{CHARACTERISTIC_ID_MIN}–{CHARACTERISTIC_ID_MAX}{truncated}: {preview}"
            )
        else:
            logger.info(
                f"All characteristic_id values within range "
                f"{CHARACTERISTIC_ID_MIN}–{CHARACTERISTIC_ID_MAX}. ✓"
            )
    else:
        logger.warning("Column 'characteristic_id' not found — advisory check 6 skipped.")

    # --- Advisory Check 7: count_total null rate -------------------------
    null_rate_warnings = []
    null_rate = 0.0
    if "count_total" not in df.columns:
        logger.warning("Column 'count_total' not found — advisory check 7 skipped.")
    if "count_total" in df.columns:
        null_rate = df["count_total"].isnull().sum() / len(df)
        if null_rate > MAX_NULL_RATE:
            null_rate_warnings.append("count_total")
            logger.warning(
                f"count_total null rate is {null_rate:.1%} — exceeds threshold of "
                f"{MAX_NULL_RATE:.1%}. This may indicate excessive suppressed values."
            )
        else:
            logger.info(f"count_total null rate: {null_rate:.1%}. ✓")

    # --- Summary ---------------------------------------------------------
    summary = {
        "rows": len(df),
        "geo_level_warnings": geo_level_warnings,
        "null_rate_warnings": null_rate_warnings,
        "null_rate": float(round(null_rate, 4)),
    }

    logger.info(f"=== Validation complete — summary: {summary} ===")
    return summary


# --- Entrypoint ----------------------------------------------------------

if __name__ == "__main__":
    import sys
    import io
    import boto3
    from dotenv import load_dotenv
    load_dotenv()

    if len(sys.argv) != 2:
        print("Usage: python -m src.pipeline.validate <silver_s3_key>")
        sys.exit(1)

    required_env = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_REGION", "S3_BUCKET_NAME"]
    missing_env = [k for k in required_env if not os.getenv(k)]
    if missing_env:
        print(f"Missing required environment variables: {missing_env}")
        sys.exit(1)

    silver_key = sys.argv[1]
    bucket = os.getenv("S3_BUCKET_NAME")

    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION"),
    )
    response = s3.get_object(Bucket=bucket, Key=silver_key)
    df = pd.read_parquet(io.BytesIO(response["Body"].read()))
    validate(df)