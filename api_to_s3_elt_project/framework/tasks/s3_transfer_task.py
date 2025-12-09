# framework/tasks/s3_transfer_task.py
from __future__ import annotations

from typing import Dict, Any, List
import logging
from datetime import datetime, date

from framework.s3_transfer_parallel import transfer_records_parallel

logger = logging.getLogger(__name__)


def _parse_run_date(run_date_str: str) -> date:
    """
    Parse YYYY-MM-DD string from Airflow (e.g. {{ ds }}) into a date.
    """
    return datetime.strptime(run_date_str, "%Y-%m-%d").date()


def s3_transfer_task(
    config: Dict[str, Any],
    records: List[Dict[str, str]],
    run_date_str: str,
) -> None:
    """
    Thin wrapper used by Airflow / CLI.
    Uses config["aws"], and a run_date string (YYYY-MM-DD) to:
      - build weekly prefix
      - check if already loaded
      - run parallel transfer if needed
    """
    aws_config = config["aws"]
    run_dt = _parse_run_date(run_date_str)

    logger.info(
        "S3 transfer task started for run_date=%s with %d records",
        run_dt.isoformat(),
        len(records),
    )
    transfer_records_parallel(aws_config, records, run_dt)
    logger.info("S3 transfer task finished successfully for run_date=%s", run_dt.isoformat())
