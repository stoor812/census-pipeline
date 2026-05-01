"""
census_dag.py — Airflow DAG for the Statistics Canada census pipeline.
Orchestrates: ingest → transform → validate → load
"""

import io
import logging
import os
import sys

import boto3
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from dotenv import load_dotenv

sys.path.insert(0, "/opt/airflow")
load_dotenv("/opt/airflow/.env")

logger = logging.getLogger(__name__)


# --- Wrapper functions -------------------------------------------------------

def ingest_wrapper(**_):
    from src.pipeline.ingest import run_ingest
    return run_ingest()


def transform_wrapper(**context):
    from src.pipeline.transform import run_transform
    bronze_key = context["ti"].xcom_pull(task_ids="ingest_task")
    return run_transform(bronze_key)


def validate_wrapper(**context):
    from src.pipeline.validate import validate
    silver_key = context["ti"].xcom_pull(task_ids="transform_task")
    bucket = os.getenv("S3_BUCKET_NAME")
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION"),
    )
    response = s3.get_object(Bucket=bucket, Key=silver_key)
    df = pd.read_parquet(io.BytesIO(response["Body"].read()))
    summary = validate(df)
    logger.info(f"Validation summary: {summary}")


def load_wrapper(**context):
    from src.pipeline.load import run_load
    silver_key = context["ti"].xcom_pull(task_ids="transform_task")
    total = run_load(silver_key)
    logger.info(f"Loaded {total} rows into census_profiles.")


# --- DAG definition ----------------------------------------------------------

with DAG(
    dag_id="census_pipeline",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    description="Statistics Canada 2021 Census Profile pipeline: ingest → transform → validate → load",
) as dag:

    ingest_task = PythonOperator(
        task_id="ingest_task",
        python_callable=ingest_wrapper,
    )

    transform_task = PythonOperator(
        task_id="transform_task",
        python_callable=transform_wrapper,
    )

    validate_task = PythonOperator(
        task_id="validate_task",
        python_callable=validate_wrapper,
    )

    load_task = PythonOperator(
        task_id="load_task",
        python_callable=load_wrapper,
    )

    ingest_task >> transform_task >> validate_task >> load_task
