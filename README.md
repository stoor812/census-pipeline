# Census Pipeline

An end-to-end data engineering pipeline that ingests the Statistics Canada 2021 Census Profile by federal electoral district, cleans and validates the data through a medallion architecture on AWS S3, loads it into PostgreSQL, and orchestrates the full run with Apache Airflow. Built for riding-level demographic analysis of Canadian federal electoral districts.

---

## Architecture

```
StatCan bulk CSV download (requests)
            │
            ▼
   AWS S3 — bronze/          ← raw parquet, all columns preserved as strings
            │
            ▼
   AWS S3 — silver/          ← cleaned parquet: columns renamed, types cast, symbols → NaN
            │
            ▼
      Validation              ← 4 critical checks + 3 advisory checks
            │
            ▼
  PostgreSQL census_profiles  ← idempotent upsert, composite PK (dguid + characteristic_id)
            │
            ▼
    Apache Airflow DAG        ← orchestrates and schedules the full pipeline
```

---

## Tech Stack

| Tool | Role |
|---|---|
| **Python / pandas** | Data ingestion, transformation, and type casting |
| **pyarrow** | Parquet serialisation for S3 reads and writes |
| **boto3** | AWS SDK — S3 uploads, downloads, and object listing |
| **SQLAlchemy + psycopg2** | PostgreSQL connection and dialect-specific upsert (`INSERT ... ON CONFLICT DO UPDATE`) |
| **Apache Airflow** | DAG orchestration — schedules tasks, tracks state, handles retries |
| **PostgreSQL** | Analytical target table, runs in Docker |
| **Docker Compose** | Runs PostgreSQL and all four Airflow services locally |
| **Click** | CLI interface for running individual pipeline phases or the full pipeline |
| **python-dotenv** | Loads AWS and DB credentials from `.env` at runtime |

---

## Project Structure

```
census-pipeline/
├── dags/
│   └── census_dag.py         # Airflow DAG — four PythonOperator tasks, linear deps
├── src/
│   └── pipeline/
│       ├── ingest.py          # Downloads StatCan zip, extracts CSV, writes bronze parquet to S3
│       ├── transform.py       # Cleans bronze, renames columns, casts types, writes silver to S3
│       ├── validate.py        # Runs critical and advisory checks on the silver DataFrame
│       ├── load.py            # Reads silver from S3, upserts into PostgreSQL
│       └── cli.py             # Click CLI: run / ingest / transform / validate / load
├── tests/
│   ├── test_transform.py      # Unit tests for transform_data() — no S3, no network
│   └── test_validate.py       # Unit tests for validate() — no S3, no network
├── docker-compose.yml         # PostgreSQL (port 5433) + Airflow stack (webserver on 8080)
├── requirements.txt
└── .env.example
```

---

## Setup

### 1. Clone and create virtual environment

```bash
git clone https://github.com/stoor812/census-pipeline.git
cd census-pipeline
python -m venv venv
source venv/Scripts/activate   # Windows Git Bash
# or
source venv/bin/activate        # macOS / Linux
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
pip install sqlalchemy psycopg2-binary pytest
```

### 3. Configure environment variables

```bash
cp .env.example .env
```

Edit `.env` and fill in your values:

```
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=us-east-2
S3_BUCKET_NAME=your-bucket-name
DB_HOST=localhost       # use 'postgres' when connecting from inside Docker
DB_PORT=5433            # use 5432 when connecting from inside Docker
DB_NAME=census_data
DB_USER=pipeline
DB_PASSWORD=pipeline
```

### 4. Start Docker services

```bash
docker compose up -d
```

This starts PostgreSQL on port `5433` and the full Airflow stack (init → webserver on `8080` + scheduler). On first run, `airflow-init` creates the metadata database and an admin user (`admin` / `admin`).

---

## How to Run

### Option A — CLI (local, outside Docker)

Run the full pipeline end to end, skipping the StatCan download and reusing the latest bronze already in S3:

```bash
python -m src.pipeline.cli run --skip-ingest
```

Run individual phases:

```bash
python -m src.pipeline.cli ingest
python -m src.pipeline.cli transform
python -m src.pipeline.cli validate
python -m src.pipeline.cli load
```

### Option B — Airflow UI

1. Open [http://localhost:8080](http://localhost:8080) and log in with `admin` / `admin`
2. Find the `census_pipeline` DAG
3. Toggle it on and click **Trigger DAG**

The DAG runs all four phases in sequence: `ingest_task → transform_task → validate_task → load_task`.

---

## Tests

```bash
pytest tests/test_transform.py tests/test_validate.py -v
```

All tests are unit tests against pure functions — no S3, no database, no network. They build minimal fake DataFrames inline and run in about one second.

---

## Key Concepts Demonstrated

- **Medallion architecture** — raw data lands in bronze exactly as downloaded; silver applies all cleaning; nothing is mutated in place. Each layer is independently queryable and replayable from S3.

- **Idempotent upserts** — the load step uses `INSERT ... ON CONFLICT (dguid, characteristic_id) DO UPDATE`, so running the pipeline multiple times on the same data produces the same result with no duplicates.

- **Airflow orchestration** — the DAG uses `PythonOperator` tasks with XCom to pass S3 keys between stages, `schedule_interval=None` for manual/triggered runs, and `catchup=False` to avoid backfilling on startup.

- **Separation of concerns** — each pipeline phase is an independently runnable module (`ingest.py`, `transform.py`, `validate.py`, `load.py`) exposed through both a CLI and an Airflow DAG, making it easy to re-run or debug any single stage without touching the others.
