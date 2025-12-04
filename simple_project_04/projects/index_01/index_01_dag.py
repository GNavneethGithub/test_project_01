import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

# ------------------------------------------------------------------------------------------
dag_updated_on = "2025-01-01T00:00:00+08:00"
# ------------------------------------------------------------------------------------------


# ------------------------------------------------------------------------------------------
PROJECT_ROOT_PATH = "/opt/airflow/dags/repo/simple_project_04"

try:
    if PROJECT_ROOT_PATH not in sys.path:
        sys.path.insert(0, PROJECT_ROOT_PATH)   
except Exception as e:
    print(f"Error inserting project root path to sys.path: {e}")


# ------------------------------------------------------------------------------------------
from framework.config_handler import main_config_handler

CONFIG_FILE_RELATIVE_PATH = "projects/index_01/config.json"


config = main_config_handler(PROJECT_ROOT_PATH, CONFIG_FILE_RELATIVE_PATH)
business_index_name = config["business_index_name"]
index_group = config["index_group"]
  

# ------------------------------------------------------------------------------------------
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException

# ========================================================================================
# SCHEDULING CONFIGURATION - CRON or INTERVAL style
# ========================================================================================

# CRON Expression (e.g., Daily at 2 AM)
DAG_SCHEDULE_INTERVAL = "0 2 * * *"

# INTERVAL in minutes (e.g., Every 60 minutes)
# DAG_SCHEDULE_INTERVAL = 60

# ========================================================================================

DAG_ID = f"{business_index_name}_dag"
DAG_OWNER = "data_engineer_team"
DAG_TAGS = [f"{index_group}", f"{business_index_name}", "ELT"]
DAG_TIMEZONE = "America/New_York"
EMAIL_ON_FAILURE = True
EMAIL_ON_RETRY = False
EMAIL_RECIPIENTS = ["XYZ@COMPANY.COM"]
DAG_MAX_ACTIVE_RUNS = 1
DAG_CATCHUP = False
DAG_MAX_RETRIES = 0
DAG_RETRY_DELAY_MINUTES = 5

DEFAULT_DAG_ARGS = {
    "owner": DAG_OWNER,
    "depends_on_past": False,
    "email_on_failure": EMAIL_ON_FAILURE,
    "email_on_retry": EMAIL_ON_RETRY,
    "email": EMAIL_RECIPIENTS,
    "retries": DAG_MAX_RETRIES,
    "retry_delay": timedelta(minutes=DAG_RETRY_DELAY_MINUTES),
}   

# ------------------------------------------------------------------------------------------
from framework.monitoring_scripts.sf_pipeline_tracking_scripts import get_next_n_records_for_data_pipeline_main

def fetch_record_to_process(**kwargs):
    """
    Fetch the next N records to process from the data pipeline queue.
    Raises AirflowException if continue_dag_run is False to stop the pipeline.
    """
    ti = kwargs['ti']
    N_record_result = get_next_n_records_for_data_pipeline_main(config)   
    # Check if we should continue
    if not N_record_result.get("continue_dag_run", False):
        error_msg = N_record_result.get("error_message", "Unknown error occurred during record fetch")
        raise AirflowException(error_msg)
    records = N_record_result.get("records", [])
    ti.xcom_push(key='pending_records_to_process', value=records) 
    return records
# ------------------------------------------------------------------------------------------
from framework.source_to_stage_scripts import generate_cmd_to_process

def transfer_source_to_stage(**kwargs):
    """
    Placeholder function for transferring data from source to staging area.
    Actual implementation would go here.
    """
    records = kwargs['ti'].xcom_pull(key='pending_records_to_process', task_ids='fetch_records_to_process')
    cmd_list = generate_cmd_to_process(config, records)

    pass


# ------------------------------------------------------------------------------------------
with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_DAG_ARGS,
    description=f"Data pipeline for {index_group}, {business_index_name}",
    schedule_interval=DAG_SCHEDULE_INTERVAL,
    start_date=datetime(2020, 1, 1),
    catchup=DAG_CATCHUP,
    max_active_runs=DAG_MAX_ACTIVE_RUNS,
    tags=DAG_TAGS,
    timezone=DAG_TIMEZONE
) as dag:

    # Start task
    start_task = DummyOperator(task_id='start_pipeline')

    # Fetch records from the pipeline queue
    fetch_records_task = PythonOperator(
        task_id='fetch_records_to_process',
        python_callable=fetch_record_to_process,
        provide_context=True
    )

    # End task
    end_task = DummyOperator(task_id='end_pipeline')

    # Define task dependencies
    start_task >> fetch_records_task >> transfer_source_to_stage_task >> end_task





