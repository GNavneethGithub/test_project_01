# my_main_project_folder/framework/logic_flow_scripts/config_handler.py

import os
import json
from typing import Optional, Dict, Any
from utils.custom_logging import CustomChainLogger
from framework.monitoring_scripts.sf_config_table_scripts import fetch_pipeline_config_details

def load_defaults(abs_path: str, logger: Optional[CustomChainLogger] = None) -> Dict[str, Any]:
    """
    Load default config from a JSON file.

    Always returns:
        {
            "continue_dag_run": bool,
            "error_message": str | None,
            "data": dict
        }

    No exceptions are raised (errors logged + returned).
    """

    log_keyword = "LOAD_DEFAULTS"
    log = logger.new_frame(log_keyword)

    # base response
    response = {
        "continue_dag_run": False,
        "error_message": None,
        "data": {}
    }

    log.info(log_keyword, f"Loading config file: {abs_path}")

    # --- File Exists Check ---
    if not os.path.exists(abs_path):
        msg = f"Config file not found: {abs_path}"
        log.error(log_keyword, msg)
        response["error_message"] = msg
        return response

    # --- Load JSON ---
    try:
        with open(abs_path, "r", encoding="utf-8") as f:
            parsed = json.load(f)

        if not isinstance(parsed, dict):
            log.debug(log_keyword, "Top-level JSON is not a dict; wrapping as dict.")
            parsed = {"wrapped_value": parsed}

        log.info(log_keyword, f"Config loaded successfully. keys={list(parsed.keys())}")

        response["continue_dag_run"] = True
        response["data"] = parsed
        return response

    except json.JSONDecodeError as e:
        msg = f"JSON parsing error: {str(e)}"
        log.error(log_keyword, msg)
        response["error_message"] = msg
        return response

    except Exception as e:
        msg = f"Unexpected error: {str(e)}"
        log.error(log_keyword, msg)
        response["error_message"] = msg
        return response



def convert_config_variant_to_dict(
    default_config: Dict[str, Any],
    logger: CustomChainLogger,
    query_tag: str
) -> Dict[str, Any]:
    """
    Fetch the CONFIG variant column from Snowflake and convert it into a Python dict.

    Steps:
    1. Use default_config to run fetch_pipeline_config_details()
    2. If a Snowflake record exists:
            - Extract CONFIG column
            - Convert CONFIG to dict (wrap if needed)
            - return continue_dag_run=True + data=dict
    3. If no record or errors:
            - return continue_dag_run=False + empty data

    Returns the unified response structure:
        {
            "continue_dag_run": bool,
            "error_message": None | str,
            "data": dict
        }
    """

    LOG_KEYWORD = "CONVERT_CONFIG_VARIANT_TO_DICT"
    log = logger.new_frame(LOG_KEYWORD)

    response = {
        "continue_dag_run": False,
        "error_message": None,
        "data": {}
    }

    log.info(LOG_KEYWORD, "Starting CONFIG variant conversion", query_tag=query_tag)

    # ------------------------------------------------------------------
    # STEP 1: Call your existing fetch function
    # ------------------------------------------------------------------
    fetch_resp = fetch_pipeline_config_details(
        config=default_config,
        logger=logger,
        query_tag=query_tag
    )

    if not fetch_resp.get("continue_dag_run", False):
        err = fetch_resp.get("error_message")
        log.error(LOG_KEYWORD, "fetch_pipeline_config_details reported failure", error_message=err)
        response["error_message"] = err
        return response

    records = fetch_resp.get("records", [])
    if not records:
        log.info(LOG_KEYWORD, "No records found in PIPELINE_CONFIG_DETAILS for given criteria")
        response["continue_dag_run"] = False
        response["data"] = {}
        return response

    # ------------------------------------------------------------------
    # STEP 2: Extract CONFIG column from first record
    # ------------------------------------------------------------------
    sf_row = records[0]

    config_variant = sf_row.get("CONFIG")
    if config_variant is None:
        log.info(LOG_KEYWORD, "CONFIG column is NULL or missing in Snowflake record")
        response["continue_dag_run"] = False
        response["data"] = {}
        return response

    # ------------------------------------------------------------------
    # STEP 3: Convert CONFIG → Python dict (always return a dict)
    # Matching load_defaults behavior (wrap non-dict JSON)
    # ------------------------------------------------------------------
    try:
        if isinstance(config_variant, dict):
            parsed = config_variant
            log.debug(LOG_KEYWORD, "CONFIG is already a dict; using as-is.")

        elif isinstance(config_variant, str):
            # Parse string JSON
            parsed = json.loads(config_variant)

            if not isinstance(parsed, dict):
                log.debug(
                    LOG_KEYWORD,
                    "CONFIG JSON string parsed; top-level not dict. Wrapping into dict."
                )
                parsed = {"wrapped_value": parsed}
            else:
                log.debug(LOG_KEYWORD, "CONFIG JSON string parsed into dict.")

        else:
            # Anything else → wrap it
            log.debug(
                LOG_KEYWORD,
                "CONFIG value not dict/string; wrapping into dict.",
                config_type=str(type(config_variant))
            )
            parsed = {"wrapped_value": config_variant}

        # ------------------------------------------------------------------
        # SUCCESS
        # ------------------------------------------------------------------
        response["continue_dag_run"] = True
        response["data"] = parsed
        log.info(LOG_KEYWORD, "CONFIG variant converted successfully.")
        return response

    except Exception as e:
        err_msg = f"Unexpected error converting CONFIG: {str(e)}"
        log.error(LOG_KEYWORD, err_msg)
        response["error_message"] = err_msg
        response["continue_dag_run"] = False
        response["data"] = {}
        return response





