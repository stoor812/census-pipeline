"""
cli.py — Census pipeline CLI
Exposes each pipeline phase as a Click command, plus a `run` command
that executes the full pipeline end to end.
"""

import io
import logging
import os

import boto3
import click
import pandas as pd
from dotenv import load_dotenv

from src.pipeline.ingest import run_ingest
from src.pipeline.transform import run_transform
from src.pipeline.validate import validate as run_validate
from src.pipeline.load import run_load

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


# --- Helper --------------------------------------------------------------

def _validate_s3_env():
    """Raise ClickException early if S3-related env vars are missing."""
    required = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_REGION", "S3_BUCKET_NAME"]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        raise click.ClickException(f"Missing required environment variables: {missing}")


def _get_latest_s3_key(prefix: str) -> str:
    """Return the S3 key of the most recently modified object with given prefix."""
    _validate_s3_env()
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION"),
    )
    bucket = os.getenv("S3_BUCKET_NAME")
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if "Contents" not in response:
        raise FileNotFoundError(f"No files found in s3://{bucket}/{prefix}")
    objects = sorted(response["Contents"], key=lambda x: x["LastModified"])
    return objects[-1]["Key"]


def _read_silver_from_s3(silver_key: str) -> pd.DataFrame:
    """Read a silver parquet from S3 and return as a DataFrame."""
    _validate_s3_env()
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION"),
    )
    bucket = os.getenv("S3_BUCKET_NAME")
    response = s3.get_object(Bucket=bucket, Key=silver_key)
    return pd.read_parquet(io.BytesIO(response["Body"].read()))


# --- CLI group -----------------------------------------------------------

@click.group()
def cli():
    """Census pipeline — Statistics Canada 2021 Census Profile (FED, 2013 Representation Order)."""
    pass


# --- Individual phase commands -------------------------------------------

@cli.command()
def ingest():
    """Download the StatCan census CSV and write to S3 bronze layer."""
    try:
        bronze_key = run_ingest()
    except Exception as e:
        raise click.ClickException(str(e)) from e
    click.echo(f"Bronze key: {bronze_key}")


@cli.command()
@click.option(
    "--bronze-key",
    default=None,
    help="S3 key of bronze parquet to transform. Defaults to latest bronze in S3.",
)
def transform(bronze_key):
    """Transform the bronze parquet and write clean silver to S3."""
    try:
        if bronze_key is None:
            bronze_key = _get_latest_s3_key("bronze/")
            logger.info(f"Using latest bronze: {bronze_key}")
        silver_key = run_transform(bronze_key)
    except Exception as e:
        raise click.ClickException(str(e)) from e
    click.echo(f"Silver key: {silver_key}")


@cli.command()
@click.option(
    "--silver-key",
    default=None,
    help="S3 key of silver parquet to validate. Defaults to latest silver in S3.",
)
def validate_cmd(silver_key):
    """Run validation checks on the silver parquet."""
    try:
        if silver_key is None:
            silver_key = _get_latest_s3_key("silver/")
            logger.info(f"Using latest silver: {silver_key}")
        df = _read_silver_from_s3(silver_key)
        summary = run_validate(df)
    except Exception as e:
        raise click.ClickException(str(e)) from e
    click.echo(f"Validation summary: {summary}")


@cli.command()
@click.option(
    "--silver-key",
    default=None,
    help="S3 key of silver parquet to load. Defaults to latest silver in S3.",
)
def load(silver_key):
    """Load the silver parquet into PostgreSQL."""
    try:
        if silver_key is None:
            silver_key = _get_latest_s3_key("silver/")
            logger.info(f"Using latest silver: {silver_key}")
        total = run_load(silver_key)
    except Exception as e:
        raise click.ClickException(str(e)) from e
    click.echo(f"Loaded {total} rows into census_profiles.")


# --- Full pipeline command -----------------------------------------------

@cli.command()
@click.option(
    "--skip-ingest",
    is_flag=True,
    default=False,
    help=(
        "Skip downloading a new file from StatCan and use the latest "
        "bronze already in S3. Useful when developing transform or load."
    ),
)
def run(skip_ingest):
    """Run the full pipeline end to end: ingest → transform → validate → load."""
    logger.info("=== Starting full census pipeline ===")

    try:
        # Step 1 — Ingest
        if skip_ingest:
            bronze_key = _get_latest_s3_key("bronze/")
            logger.info(f"Skipping ingest — using latest bronze: {bronze_key}")
        else:
            bronze_key = run_ingest()
        click.echo(f"Bronze key: {bronze_key}")

        # Step 2 — Transform
        silver_key = run_transform(bronze_key)
        click.echo(f"Silver key: {silver_key}")

        # Step 3 — Validate
        df = _read_silver_from_s3(silver_key)
        summary = run_validate(df)
        click.echo(f"Validation summary: {summary}")

        # Step 4 — Load
        total = run_load(silver_key)
        click.echo(f"Loaded {total} rows into census_profiles.")
    except click.ClickException:
        raise
    except Exception as e:
        raise click.ClickException(str(e)) from e

    logger.info("=== Full pipeline complete ===")


# --- Entrypoint ----------------------------------------------------------

if __name__ == "__main__":
    cli()