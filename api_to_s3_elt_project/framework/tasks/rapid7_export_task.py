# framework/tasks/rapid7_export_task.py
from __future__ import annotations

from typing import Dict, Any, List
import logging

from framework.rapid7_client import run_rapid7_export

logger = logging.getLogger(__name__)

def rapid7_export_task(config: Dict[str, Any]) -> List[Dict[str, str]]:
    """
    Thin wrapper used by Airflow / CLI.
    Uses config["api_"] and returns list[{"url": ..}, {"prefix": ..}].
    """
    api_config = config["api_"]
    logger.info("Rapid7 export task started")
    records = run_rapid7_export(api_config)
    logger.info("Rapid7 export task finished with %d records", len(records))
    return records


