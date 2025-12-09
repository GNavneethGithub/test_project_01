# users/project_01/project_01_dag.py
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from framework.config_loader import load_project_config
from framework.logging_setup import setup_logging
from framework.tasks.rapid7_export_task import rapid7_export_task
from framework.tasks.s3_transfer_task import s3_transfer_task

# Adjust path as needed in your Airflow environment
PROJECT_CONFIG_PATH = "/opt/airflow/dags/users/project_01/config.json"

# Load config and set up logging once at import time
config = load_project_config(PROJECT_CONFIG_PATH)
logger = setup_logging(config)


def rapid7_export_callable(**context):
    """
    Airflow Task 1 callable.
    Returns list[dict] -> stored in XCom.
    """
    return rapid7_export_task(config)


def s3_transfer_callable(run_date: str, **context):
    """
    Airflow Task 2 callable.
    Pulls records from XCom of Task 1 and runs weekly parallel S3 transfer.
    run_date is a YYYY-MM-DD string (e.g. {{ ds }}) used to compute year/month/week.
    """
    ti = context["ti"]
    records = ti.xcom_pull(task_ids="rapid7_export")

    # records should be list[dict[str, str]]
    if not records:
        logger.warning("No records received from rapid7_export – nothing to transfer.")
        return

    s3_transfer_task(config, records, run_date_str=run_date)


default_args = {
    "owner": "project_01",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="project_01_rapid7_to_s3",
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 1 * * 1",   # ✅ Every Monday at 01:00
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
) as dag:

    t1 = PythonOperator(
        task_id="rapid7_export",
        python_callable=rapid7_export_callable,
    )

    t2 = PythonOperator(
        task_id="s3_transfer",
        python_callable=s3_transfer_callable,
        op_kwargs={"run_date": "{{ ds }}"},  # ds = logical date (YYYY-MM-DD)
    )

    t1 >> t2
