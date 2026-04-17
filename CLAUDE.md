# Claude Code Instructions

## Git
When asked to push changes:
1. Run `git add .`
2. Run `git commit -m "[descriptive message based on what changed]"`
3. Run `git push origin main`

Always write descriptive commit messages that explain what changed and why.
Never push if there are uncommitted changes to .env (it contains secrets).
Never push if there are uncommitted changes to any file containing AWS credentials.

## Project Context
This is a Data Engineering pipeline for Statistics Canada 2021 Census data
by federal electoral district, built for political riding-level demographic analysis.

Stack: Python, pandas, pyarrow, boto3, Apache Airflow, PostgreSQL, SQLAlchemy, Docker.

Architecture: 
- Bronze (raw parquet) → stored in AWS S3
- Silver (clean parquet) → stored in AWS S3  
- PostgreSQL → final analytical table loaded via idempotent upsert
- Airflow DAG → orchestrates and schedules the full pipeline

## Key Concepts
- DAG file lives in dags/census_dag.py
- Pipeline logic lives in src/pipeline/
- Airflow runs in Docker via docker-compose.yml
- AWS credentials loaded from .env — never hardcoded
- Bronze and silver layers write to S3 not local disk

## AWS
- Never hardcode AWS credentials anywhere in code
- Always load credentials from environment variables
- S3 bucket name is defined in .env
- Use boto3 for all S3 interactions