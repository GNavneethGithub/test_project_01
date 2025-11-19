"""
Function: create_backfill_requests_table
Location: my_main_project_folder/framework/backfill_requests_manager.py
Purpose: Simplified wrapper for creating BACKFILL_REQUESTS table
"""

from typing import Dict, Any
from utils.custom_logging import CustomChainLogger
from framework.generic_table_creator import create_table_generic


def create_backfill_requests_table(
    config: Dict[str, Any],
    logger: CustomChainLogger
) -> Dict[str, Any]:
    """
    Creates the BACKFILL_REQUESTS table in Snowflake if it does not already exist.
    
    This is a simplified wrapper around create_table_generic() that handles
    backfill requests table creation.
    
    Args:
        config (Dict[str, Any]): Configuration dictionary containing:
                      - config["sf_con_parms"]: Snowflake connection params
                      - config["backfill_requests_details"]: Backfill requests details with:
                          - backfill_requests_table_name
                          - backfill_requests_database_name
                          - backfill_requests_schema_name
                          - backfill_requests_warehouse_name
                          - backfill_requests_role_name
        logger (CustomChainLogger): Our custom logger for logging operations.
    
    Returns:
        Dict[str, Any]: A dictionary containing:
            - 'success' (bool): True if table creation/verification succeeded
            - 'message' (str): Descriptive message about the operation
            - 'continue_dag_run' (bool): Whether the DAG should continue execution
            - 'error_message' (str): Error details if operation failed
            - 'cleanup_errors' (List[str]): List of cleanup errors if any occurred
    """
    
    # SQL template with named parameters (for Snowflake API)
    create_table_sql = """
CREATE TABLE IF NOT EXISTS %(table_name)s (
    backfill_request_id VARCHAR NOT NULL,
    pipeline_name VARCHAR NOT NULL,
    request_data_window_start_timestamp TIMESTAMP_TZ NOT NULL,
    request_data_window_end_timestamp TIMESTAMP_TZ NOT NULL,
    request_window_interval_description VARCHAR,
    backfill_request_status VARCHAR NOT NULL,
    requested_by_identifier VARCHAR NOT NULL,
    request_initiated_timestamp TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    backfill_completion_timestamp TIMESTAMP_TZ,
    backfill_duration_seconds NUMBER,
    completion_percentage NUMBER DEFAULT 0,
    is_stop_requested BOOLEAN DEFAULT FALSE,
    stop_reason VARCHAR,
    created_at TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (backfill_request_id)
);
    """
    
    # Extract required parameters from config
    sf_con_parms = config.get("sf_con_parms")
    backfill_requests_details = config.get("backfill_requests_details")
    
    # Normalize table_details keys (remove prefix for generic function)
    table_details: Dict[str, Any] = {}
    if backfill_requests_details:
        table_details = {
            'table_name': backfill_requests_details.get("backfill_requests_table_name"),
            'database_name': backfill_requests_details.get("backfill_requests_database_name"),
            'schema_name': backfill_requests_details.get("backfill_requests_schema_name"),
            'warehouse_name': backfill_requests_details.get("backfill_requests_warehouse_name"),
            'role_name': backfill_requests_details.get("backfill_requests_role_name")
        }
    
    # Call generic table creator
    return create_table_generic(
        sf_con_parms=sf_con_parms,
        table_details=table_details,
        log_keyword="CREATE_BACKFILL_REQUESTS_TABLE",
        new_frame_name="create_backfill_requests_table",
        create_table_sql=create_table_sql,
        logger=logger,
        config=config
    )



