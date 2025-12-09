# framework/tasks/week_check_task.py
from __future__ import annotations

import logging
from datetime import datetime, date
from typing import Dict, Any

from airflow.exceptions import AirflowSkipException

from framework.s3_transfer_parallel import is_week_already_loaded

logger = logging.getLogger(__name__)


def _parse_run_date(run_date_str: str) -> date:
    """
    Parse YYYY-MM-DD string from Airflow (e.g. {{ ds }}) into a date.
    """
    return datetime.strptime(run_date_str, "%Y-%m-%d").date()


def week_check_task(config: Dict[str, Any], run_date_str: str) -> None:
    """
    First task in the DAG:
      - Compute this run's ISO week prefix
      - Check if data already exists in S3 for that week
      - If yes -> skip the rest of the DAG using AirflowSkipException
      - If no  -> allow downstream tasks to run
    """
    aws_config = config["aws"]
    run_dt = _parse_run_date(run_date_str)

    logger.info("Week check task started for run_date=%s", run_dt.isoformat())

    already_loaded = is_week_already_loaded(aws_config, run_dt)

    if already_loaded:
        logger.info(
            "Weekly data already exists in target S3 for run_date=%s. "
            "Skipping downstream tasks.",
            run_dt.isoformat(),
        )
        # This tells Airflow: mark this task as SKIPPED and skip all downstream
        raise AirflowSkipException("Weekly data already loaded; skipping pipeline.")
    else:
        logger.info(
            "No existing weekly data found for run_date=%s. Proceeding with pipeline.",
            run_dt.isoformat(),
        )


def week_check_branch_task(config: Dict[str, Any], run_date_str: str) -> str:
    """
    Branching task:
      - If week already exists → go to 'skip_pipeline'
      - If not → go to 'rapid7_export'
    """
    aws_config = config["aws"]
    run_dt = _parse_run_date(run_date_str)

    logger.info("Week check (branch) started for run_date=%s", run_dt.isoformat())

    already_loaded = is_week_already_loaded(aws_config, run_dt)

    if already_loaded:
        logger.info(
            "Weekly data already exists for run_date=%s -> branching to SKIP.",
            run_dt.isoformat(),
        )
        return "skip_pipeline"
    else:
        logger.info(
            "No weekly data found for run_date=%s -> branching to EXPORT.",
            run_dt.isoformat(),
        )
        return "rapid7_export"


