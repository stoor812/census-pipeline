"""
ingest.py — Census pipeline bronze layer ingestion
Downloads the Statistics Canada 2021 Census Profile CSV (FED, 2013 Representation Order),
extracts it from the zip, and writes it as parquet to S3 bronze layer.
"""

import io
import logging
import os
import zipfile
from datetime import datetime

import boto3
import pandas as pd
import requests
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)
load_dotenv()

# --- Constants -----------------------------------------------------------

CENSUS_URL = (
    "https://www12.statcan.gc.ca/census-recensement/2021/dp-pd/prof/details/"
    "download-telecharger/comp/GetFile.cfm?Lang=E&FILETYPE=CSV&GEONO=010"
)
CHUNK_SIZE = 8192  # bytes per chunk when streaming the download

def _get_s3_bucket() -> str:
    """Return S3 bucket name from environment, resolved at call time."""
    return os.getenv("S3_BUCKET_NAME")

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
        region_name=os.getenv("AWS_REGION"),
    )


# --- Step 1: Download ----------------------------------------------------

def download_census_zip(url: str = CENSUS_URL) -> bytes:
    """
    Stream-download the StatCan zip file and return its raw bytes.
    Streams in chunks to avoid loading a large file into memory at once.

    Args:
        url: Direct download URL for the StatCan FED CSV zip.

    Returns:
        Raw bytes of the zip file.

    Raises:
        requests.HTTPError: If the HTTP response is not 2xx.
        ValueError: If the response doesn't look like a zip file.
    """
    logger.info("Downloading census zip from StatCan...")
    response = requests.get(url, stream=True, timeout=120)
    response.raise_for_status()

    buffer = io.BytesIO()
    total_bytes = 0
    for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
        if chunk:
            buffer.write(chunk)
            total_bytes += len(chunk)

    zip_bytes = buffer.getvalue()

    # Sanity check — zip files start with PK magic bytes
    if not zip_bytes[:2] == b"PK":
        raise ValueError(
            f"Downloaded file does not appear to be a zip. "
            f"First bytes: {zip_bytes[:20]}. "
            f"The URL may have returned an error page instead of the file."
        )

    logger.info(f"Download complete — {total_bytes / 1_000_000:.1f} MB received.")
    return zip_bytes


# --- Step 2: Extract CSV from zip ----------------------------------------

def extract_csv_from_zip(zip_bytes: bytes) -> pd.DataFrame:
    """
    Extract the data CSV from the StatCan zip and read it into a DataFrame.
    StatCan zips typically contain one data CSV and one metadata CSV.
    We want the data file (not the metadata file).

    Args:
        zip_bytes: Raw bytes of the downloaded zip file.

    Returns:
        DataFrame containing the raw census data.

    Raises:
        FileNotFoundError: If no suitable CSV is found inside the zip.
    """
    logger.info("Extracting CSV from zip...")

    try:
        zf_context = zipfile.ZipFile(io.BytesIO(zip_bytes))
    except zipfile.BadZipFile as e:
        raise zipfile.BadZipFile(
            f"Downloaded file is not a valid zip archive: {e}"
        ) from e

    with zf_context as zf:
        all_files = zf.namelist()
        logger.info(f"Files inside zip: {all_files}")

        # StatCan zips contain a data CSV and a metadata CSV.
        # The metadata file contains "_meta" in its name — we want the other one.
        data_files = [
            f for f in all_files
            if f.lower().endswith(".csv") and "_meta" not in f.lower()
        ]

        if not data_files:
            raise FileNotFoundError(
                f"No data CSV found in zip. Contents: {all_files}"
            )

        target_file = data_files[0]
        logger.info(f"Reading data file: {target_file}")

        with zf.open(target_file) as csv_file:
            df = pd.read_csv(
                csv_file,
                encoding="latin-1",
                dtype=str,        # read everything as string — preserve leading zeros
                low_memory=False,
            )

    if df.empty:
        raise ValueError(
            f"Extracted CSV '{target_file}' contains no rows."
        )

    logger.info(f"Extracted DataFrame shape: {df.shape} ({len(df.columns)} columns)")
    return df


# --- Step 3: Write parquet to S3 bronze ----------------------------------

def save_bronze(df: pd.DataFrame) -> str:
    """
    Write the raw DataFrame to S3 as a timestamped parquet file in the bronze layer.

    Args:
        df: Raw census DataFrame from extract_csv_from_zip.

    Returns:
        The S3 key the parquet was written to.

    Raises:
        boto3 exceptions on S3 write failure.
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    s3_key = f"bronze/census_fed_2013_{timestamp}.parquet"
    bucket = _get_s3_bucket()

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

    logger.info(f"Bronze layer written: s3://{bucket}/{s3_key}")
    return s3_key


# --- Orchestrator --------------------------------------------------------

def run_ingest() -> str:
    """
    Run the full ingestion sequence:
      1. Download zip from StatCan
      2. Extract data CSV from zip
      3. Write raw parquet to S3 bronze layer

    Returns:
        S3 key of the written bronze parquet.
    """
    _validate_env()
    logger.info("=== Starting census ingestion ===")

    zip_bytes = download_census_zip()
    df = extract_csv_from_zip(zip_bytes)
    s3_key = save_bronze(df)

    logger.info(f"=== Ingestion complete — bronze key: {s3_key} ===")
    return s3_key


# --- Entrypoint ----------------------------------------------------------

if __name__ == "__main__":
    run_ingest()
