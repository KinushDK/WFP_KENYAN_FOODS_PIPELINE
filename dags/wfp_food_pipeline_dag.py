"""
WFP Food Prices Pipeline DAG
=============================
Converts wfp_clean.ipynb notebook into an Airflow DAG.

Pipeline Steps:
    1. extract   → Read CSV from disk
    2. transform → Clean & validate data
    3. load      → Upload to PostgreSQL (wfp_food_prices_clean table)

Connection:
    Uses Airflow connection ID: wfp_postgres
    (defined in docker-compose.yaml as AIRFLOW_CONN_WFP_POSTGRES)
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator  # type: ignore[import-untyped]
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import logging

# ─────────────────────────────────────────────
# DAG DEFAULT ARGUMENTS
# ─────────────────────────────────────────────
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────
CSV_FILE_PATH = "/opt/airflow/dags/data/wfp_food_prices_ken.csv"
TABLE_NAME    = "wfp_food_prices_clean"

# PostgreSQL connection — matches your local Windows PostgreSQL
# host.docker.internal routes from Docker container → Windows host
DB_CONFIG = {
    "user":     "postgres",
    "password": "sekonda",
    "host":     "host.docker.internal",   # ← Key: reaches your local Windows PostgreSQL
    "port":     "5432",
    "database": "wfp_foods",
}


# ─────────────────────────────────────────────
# TASK 1: EXTRACT
# Reads CSV and pushes raw data to XCom
# ─────────────────────────────────────────────
def extract(**context):
    logging.info("Starting extraction from CSV...")

    df = pd.read_csv(CSV_FILE_PATH, encoding="latin1")

    logging.info(f"Extracted {len(df)} rows from CSV.")
    logging.info(f"Columns: {list(df.columns)}")

    # Push to XCom as JSON so the next task can pick it up
    context["ti"].xcom_push(key="raw_data", value=df.to_json(date_format="iso"))

    logging.info("Extraction complete.")


# ─────────────────────────────────────────────
# TASK 2: TRANSFORM
# Cleans data and pushes clean data to XCom
# ─────────────────────────────────────────────
def transform(**context):
    logging.info("Starting transformation...")

    # Pull raw data from previous task
    raw_json = context["ti"].xcom_pull(key="raw_data", task_ids="extract")
    df = pd.read_json(raw_json)

    # Step 1: Standardise column names — lowercase + underscores
    df.columns = df.columns.str.lower().str.replace(" ", "_")
    logging.info(f"Columns after rename: {list(df.columns)}")

    # Step 2: Remove duplicate rows
    before = len(df)
    df = df.drop_duplicates()
    logging.info(f"Removed {before - len(df)} duplicate rows.")

    # Step 3: Drop rows with missing commodity or price
    before = len(df)
    df = df.dropna(subset=["commodity", "price"])
    logging.info(f"Removed {before - len(df)} rows with missing commodity/price.")

    # Step 4: Convert date column to datetime
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    # Step 5: Drop rows where date conversion failed
    before = len(df)
    df = df.dropna(subset=["date"])
    logging.info(f"Removed {before - len(df)} rows with invalid dates.")

    # Step 6: Ensure price is numeric
    df["price"] = pd.to_numeric(df["price"], errors="coerce")

    # Step 7: Drop rows with invalid price values
    before = len(df)
    df = df.dropna(subset=["price"])
    logging.info(f"Removed {before - len(df)} rows with invalid price values.")

    logging.info(f"Transformation complete. {len(df)} clean rows ready to load.")

    # Push clean data to XCom
    context["ti"].xcom_push(
        key="clean_data",
        value=df.to_json(date_format="iso")
    )


# ─────────────────────────────────────────────
# TASK 3: LOAD
# Uploads clean DataFrame to PostgreSQL
# ─────────────────────────────────────────────
def load(**context):
    logging.info("Starting load to PostgreSQL...")

    # Pull clean data from previous task
    clean_json = context["ti"].xcom_pull(key="clean_data", task_ids="transform")
    df = pd.read_json(clean_json)

    # Rebuild datetime column after JSON serialisation
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    # Build connection string
    db_url = (
        f"postgresql+psycopg2://{DB_CONFIG['user']}:"
        f"{DB_CONFIG['password']}@"
        f"{DB_CONFIG['host']}:"
        f"{DB_CONFIG['port']}/"
        f"{DB_CONFIG['database']}"
    )

    engine = create_engine(db_url)

    # Load to PostgreSQL — replace table each run
    rows = df.to_sql(
        TABLE_NAME,
        engine,
        if_exists="append",
        index=False
    )

    logging.info(f"Successfully loaded {rows} rows into PostgreSQL table: {TABLE_NAME}")
    engine.dispose()


# ─────────────────────────────────────────────
# DAG DEFINITION
# ─────────────────────────────────────────────
with DAG(
    dag_id="wfp_food_prices_pipeline",
    description="ETL pipeline: Extract WFP Kenya food prices CSV → Clean → Load to PostgreSQL",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",                 # Airflow 3.x uses 'schedule' not 'schedule_interval'
    catchup=False,                     # Don't backfill historical runs
    tags=["wfp", "food-prices", "kenya", "etl", "postgresql"],
) as dag:

    # ── Task 1: Extract ──────────────────────
    task_extract = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    # ── Task 2: Transform ────────────────────
    task_transform = PythonOperator(
        task_id="transform",
        python_callable=transform,
    )

    # ── Task 3: Load ─────────────────────────
    task_load = PythonOperator(
        task_id="load",
        python_callable=load,
    )

    # ── Pipeline Order ───────────────────────
    # extract → transform → load
    task_extract >> task_transform >> task_load
