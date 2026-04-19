"""
load.py — Census pipeline PostgreSQL load
Reads clean silver parquet from S3 and upserts into the census_profiles table.
"""

import io
import logging
import os

import boto3
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import (
    Column,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    create_engine,
)
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import PrimaryKeyConstraint

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# --- Constants -----------------------------------------------------------

BATCH_SIZE = 1000

metadata = MetaData()

census_profiles = Table(
    "census_profiles",
    metadata,
    Column("dguid", String, nullable=False),
    Column("characteristic_id", Integer, nullable=False),
    Column("census_year", Integer),
    Column("alt_geo_code", String),
    Column("geo_level", String),
    Column("geo_name", String),
    Column("characteristic_name", Text),
    Column("characteristic_note", Float),
    Column("count_total", Float),
    Column("count_men", Float),
    Column("count_women", Float),
    Column("rate_total", Float),
    Column("rate_men", Float),
    Column("rate_women", Float),
    Column("data_quality_flag", Integer),
    PrimaryKeyConstraint("dguid", "characteristic_id"),
)


# --- Validation ----------------------------------------------------------

def _validate_env():
    """Raise early if any required environment variables are missing."""
    required = [
        "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_REGION", "S3_BUCKET_NAME",
        "DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD",
    ]
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
        region_name=os.getenv("AWS_REGION"),
    )


# --- Step 1: Database engine ---------------------------------------------

def get_engine():
    """
    Build a SQLAlchemy engine from environment variables.

    Returns:
        SQLAlchemy engine connected to the census PostgreSQL database.
    """
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    name = os.getenv("DB_NAME")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{name}"
    logger.info(f"Connecting to PostgreSQL at {host}:{port}/{name}...")
    return create_engine(url)


# --- Step 2: Create table ------------------------------------------------

def create_table(engine):
    """
    Create the census_profiles table if it doesn't already exist.
    Safe to call on every run — create_all is idempotent.

    Args:
        engine: SQLAlchemy engine from get_engine().
    """
    metadata.create_all(engine)
    logger.info("Table census_profiles created or already exists.")


# --- Step 3: Upsert ------------------------------------------------------

def upsert(df: pd.DataFrame, engine) -> int:
    """
    Upsert the silver DataFrame into census_profiles in batches.

    Uses INSERT ... ON CONFLICT (dguid, characteristic_id) DO UPDATE so that
    re-running the pipeline produces the same result — idempotent by design.

    Args:
        df: Clean silver DataFrame from transform.py.
        engine: SQLAlchemy engine from get_engine().

    Returns:
        Total number of rows upserted.
    """
    # Only keep columns that exist in both the DataFrame and the table schema.
    table_columns = {col.name for col in census_profiles.columns}
    df = df[[col for col in df.columns if col in table_columns]]

    # Replace all pandas NA types (np.nan, pd.NA, pd.NaT) with None so
    # PostgreSQL receives NULL. df.where + mask covers nullable-typed columns
    # that df.replace({np.nan: None}) misses.
    df = df.astype(object).where(df.notna(), other=None)

    # Cast characteristic_id to plain Python int for SQLAlchemy compatibility.
    # pandas Int64 (nullable) doesn't serialize cleanly to psycopg2.
    if "characteristic_id" in df.columns:
        df["characteristic_id"] = df["characteristic_id"].astype(object).where(
            df["characteristic_id"].notna(), None
        )
        df["characteristic_id"] = df["characteristic_id"].apply(
            lambda x: int(x) if x is not None else None
        )

    if df.empty:
        logger.warning("Silver DataFrame is empty — nothing to load into census_profiles.")
        return 0

    records = df.to_dict(orient="records")
    total = len(records)
    processed = 0

    # Columns to update on conflict — intersection of table schema and DataFrame
    # columns, excluding the primary key columns.
    pk_cols = {"dguid", "characteristic_id"}
    update_columns = [col for col in df.columns if col in table_columns and col not in pk_cols]

    with engine.begin() as conn:
        for i in range(0, total, BATCH_SIZE):
            batch = records[i: i + BATCH_SIZE]
            stmt = insert(census_profiles).values(batch)
            stmt = stmt.on_conflict_do_update(
                index_elements=["dguid", "characteristic_id"],
                set_={col: stmt.excluded[col] for col in update_columns},
            )
            conn.execute(stmt)
            processed += len(batch)
            logger.info(f"Processed {processed}/{total} rows...")

    logger.info(f"Load complete — {processed} rows processed into census_profiles.")
    return processed


# --- Step 4: Read silver from S3 -----------------------------------------

def _read_silver(s3_key: str) -> pd.DataFrame:
    """
    Read a silver parquet file from S3 into a DataFrame.

    Args:
        s3_key: S3 key of the silver parquet produced by transform.py.

    Returns:
        Clean silver DataFrame.
    """
    bucket = os.getenv("S3_BUCKET_NAME")
    logger.info(f"Reading silver parquet from s3://{bucket}/{s3_key}...")

    s3 = _get_s3_client()
    response = s3.get_object(Bucket=bucket, Key=s3_key)
    parquet_bytes = response["Body"].read()

    df = pd.read_parquet(io.BytesIO(parquet_bytes))
    logger.info(f"Silver read complete — {len(df)} rows, {len(df.columns)} columns.")
    return df


# --- Orchestrator --------------------------------------------------------

def run_load(silver_key: str) -> int:
    """
    Run the full load sequence:
      1. Read silver parquet from S3
      2. Create census_profiles table if it doesn't exist
      3. Upsert all rows into PostgreSQL

    Args:
        silver_key: S3 key of the silver parquet produced by transform.py.

    Returns:
        Total number of rows upserted.
    """
    _validate_env()
    logger.info("=== Starting census load ===")

    df = _read_silver(silver_key)
    engine = get_engine()
    create_table(engine)
    total = upsert(df, engine)

    logger.info(f"=== Load complete — {total} rows upserted ===")
    return total


# --- Entrypoint ----------------------------------------------------------

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: python -m src.pipeline.load <silver_s3_key>")
        sys.exit(1)
    run_load(sys.argv[1])