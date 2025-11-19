"""
Function: create_pipeline_execution_tracking_table
Location: my_main_project_folder/framework/pipeline_tracking_manager.py
Purpose: Simplified wrapper for creating PIPELINE_EXECUTION_TRACKING table
"""

from typing import Dict, Any
from utils.custom_logging import CustomChainLogger
from framework.generic_table_creator import create_table_generic


def create_pipeline_execution_tracking_table(
    config: Dict[str, Any],
    logger: CustomChainLogger
) -> Dict[str, Any]:
    """
    Creates the PIPELINE_EXECUTION_TRACKING table in Snowflake if it does not already exist.
    
    This is a simplified wrapper around create_table_generic() that handles
    pipeline execution tracking table creation.
    
    Args:
        config (Dict[str, Any]): Configuration dictionary containing:
                      - config["sf_con_parms"]: Snowflake connection params
                      - config["pipeline_tracking_details"]: Pipeline tracking details with:
                          - pipeline_tracking_table_name
                          - pipeline_tracking_database_name
                          - pipeline_tracking_schema_name
                          - pipeline_tracking_warehouse_name
                          - pipeline_tracking_role_name
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
    TRACKING_ID VARCHAR NOT NULL,
    PIPELINE_ID VARCHAR NOT NULL,
    PIPELINE_NAME VARCHAR NOT NULL,
    QUERY_WINDOW_START_TIMESTAMP TIMESTAMP_TZ NOT NULL,
    QUERY_WINDOW_END_TIMESTAMP TIMESTAMP_TZ NOT NULL,
    WINDOW_INTERVAL VARCHAR,
    PIPELINE_START_TIMESTAMP TIMESTAMP_TZ NOT NULL,
    PIPELINE_END_TIMESTAMP TIMESTAMP_TZ,
    PIPELINE_STATUS VARCHAR NOT NULL,
    PIPELINE_RETRY_ATTEMPT NUMBER NOT NULL DEFAULT 0,
    BACKFILL_REQUEST_ID VARCHAR DEFAULT NULL,
    COMPLETED_PHASES VARIANT,
    PENDING_PHASES VARIANT,
    RUNNING_PHASE VARCHAR,
    FAILED_PHASE VARCHAR,
    SKIPPED_PHASES VARIANT,
    CREATED_AT TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    CREATED_BY VARCHAR NOT NULL,
    PRIMARY KEY (TRACKING_ID, PIPELINE_RETRY_ATTEMPT)
);
    """
    
    # Extract required parameters from config
    sf_con_parms = config.get("sf_con_parms")
    pipeline_tracking_details = config.get("pipeline_tracking_details")
    
    # Normalize table_details keys (remove prefix for generic function)
    table_details: Dict[str, Any] = {}
    if pipeline_tracking_details:
        table_details = {
            'table_name': pipeline_tracking_details.get("pipeline_tracking_table_name"),
            'database_name': pipeline_tracking_details.get("pipeline_tracking_database_name"),
            'schema_name': pipeline_tracking_details.get("pipeline_tracking_schema_name"),
            'warehouse_name': pipeline_tracking_details.get("pipeline_tracking_warehouse_name"),
            'role_name': pipeline_tracking_details.get("pipeline_tracking_role_name")
        }
    
    # Call generic table creator
    return create_table_generic(
        sf_con_parms=sf_con_parms,
        table_details=table_details,
        log_keyword="CREATE_PIPELINE_EXECUTION_TRACKING_TABLE",
        new_frame_name="create_pipeline_execution_tracking_table",
        create_table_sql=create_table_sql,
        logger=logger,
        config=config
    )



"""
Function: fetch_pending_pipeline_executions
Location: my_main_project_folder/framework/pipeline_tracking_manager.py
Purpose: Fetch pending pipeline executions with max retry attempt, ordered by query window start timestamp
"""

from typing import Dict, Any, List
from snowflake.connector import DictCursor
from utils.custom_logging import CustomChainLogger
from utils.sql_formatter_utility import format_and_print_sql_query
from framework.snowflake_connector import get_snowflake_connection


def fetch_pending_pipeline_executions(
    config: Dict[str, Any],
    logger: CustomChainLogger
) -> Dict[str, Any]:
    """
    Fetches pending pipeline executions with maximum retry attempt for each tracking_id.
    
    Retrieves up to N pending pipeline execution records ordered by query_window_start_timestamp
    in ascending order. For each tracking_id, only the record with the highest pipeline_retry_attempt
    is returned.
    
    Args:
        config (Dict[str, Any]): Configuration dictionary containing:
                      - config["sf_con_parms"]: Snowflake connection params
                      - config["pipeline_tracking_details"]: Pipeline tracking details with:
                          - pipeline_tracking_table_name
                          - pipeline_tracking_database_name
                          - pipeline_tracking_schema_name
                          - pipeline_tracking_warehouse_name
                          - pipeline_tracking_role_name
                      - config["max_pending_records_fetch_limit"]: Maximum number of records to fetch
        logger (CustomChainLogger): Our custom logger for logging operations.
    
    Returns:
        Dict[str, Any]: A dictionary containing:
            - 'continue_dag_run' (bool): Whether the DAG should continue execution
            - 'error_message' (str): Error details if operation failed (None if successful)
            - 'records' (List[Dict]): List of pending pipeline execution records
            - 'records_count' (int): Number of records returned
    """
    
    log = logger.new_frame("fetch_pending_pipeline_executions")
    
    return_object: Dict[str, Any] = {
        'continue_dag_run': False,
        'error_message': None,
        'records': [],
        'records_count': 0
    }
    
    conn = None
    cursor = None
    dict_cursor = None
    
    try:
        # Extract Snowflake connection parameters from config
        sf_con_parms = config.get("sf_con_parms")
        pipeline_tracking_details = config.get("pipeline_tracking_details")
        max_pending_records_fetch_limit = config.get("max_pending_records_fetch_limit")
        
        # Collect all validation errors
        validation_errors: List[str] = []
        
        if not sf_con_parms:
            validation_errors.append("Missing 'sf_con_parms' in config dictionary")
        
        if not pipeline_tracking_details:
            validation_errors.append("Missing 'pipeline_tracking_details' in config dictionary")
        
        if not max_pending_records_fetch_limit:
            validation_errors.append("Missing 'max_pending_records_fetch_limit' in config dictionary")
        
        # Extract pipeline tracking details with specific names
        pipeline_tracking_table_name = pipeline_tracking_details.get("pipeline_tracking_table_name") if pipeline_tracking_details else None
        pipeline_tracking_database_name = pipeline_tracking_details.get("pipeline_tracking_database_name") if pipeline_tracking_details else None
        pipeline_tracking_schema_name = pipeline_tracking_details.get("pipeline_tracking_schema_name") if pipeline_tracking_details else None
        pipeline_tracking_warehouse_name = pipeline_tracking_details.get("pipeline_tracking_warehouse_name") if pipeline_tracking_details else None
        pipeline_tracking_role_name = pipeline_tracking_details.get("pipeline_tracking_role_name") if pipeline_tracking_details else None
        
        if not pipeline_tracking_table_name:
            validation_errors.append("Missing 'pipeline_tracking_table_name' in pipeline_tracking_details")
        
        if not pipeline_tracking_database_name:
            validation_errors.append("Missing 'pipeline_tracking_database_name' in pipeline_tracking_details")
        
        if not pipeline_tracking_schema_name:
            validation_errors.append("Missing 'pipeline_tracking_schema_name' in pipeline_tracking_details")
        
        if not pipeline_tracking_warehouse_name:
            validation_errors.append("Missing 'pipeline_tracking_warehouse_name' in pipeline_tracking_details")
        
        if not pipeline_tracking_role_name:
            validation_errors.append("Missing 'pipeline_tracking_role_name' in pipeline_tracking_details")
        
        # If any validation errors, collect them and exit
        if validation_errors:
            error_details = "\n".join(validation_errors)
            error_msg_for_email = (
                f"Fetch Pending Pipeline Executions Failed - Configuration Validation Error!\n\n"
                f"Error Location: fetch_pending_pipeline_executions\n"
                f"Error Type: ConfigurationError\n\n"
                f"Validation Errors:\n{error_details}"
            )
            
            log.error(
                "FETCH_PENDING_PIPELINE_EXECUTIONS",
                "Configuration validation failed.",
                validation_errors=error_details
            )
            
            return_object['error_message'] = error_msg_for_email
            return return_object
        
        log.info(
            "FETCH_PENDING_PIPELINE_EXECUTIONS",
            "Attempting to establish Snowflake connection for fetching pending executions...",
            account=sf_con_parms.get("account"),
            username=sf_con_parms.get("username"),
            database=pipeline_tracking_database_name,
            schema=pipeline_tracking_schema_name,
            warehouse=pipeline_tracking_warehouse_name,
            role=pipeline_tracking_role_name,
            table_name=pipeline_tracking_table_name,
            fetch_limit=max_pending_records_fetch_limit
        )
        
        # Get Snowflake connection
        conn_result = get_snowflake_connection(sf_con_parms, log)
        
        if not conn_result['continue_dag_run']:
            log.error(
                "FETCH_PENDING_PIPELINE_EXECUTIONS",
                "Failed to establish Snowflake connection.",
                error_message=conn_result['error_message']
            )
            return_object['error_message'] = conn_result['error_message']
            return return_object
        
        conn = conn_result['conn']
        cursor = conn_result['cursor']
        
        # Create a DictCursor for easy dict conversion
        dict_cursor = conn.cursor(DictCursor)
        
        log.info(
            "FETCH_PENDING_PIPELINE_EXECUTIONS",
            "Snowflake connection established successfully."
        )
        
        # Set the role, warehouse, database, and schema
        log.info(
            "FETCH_PENDING_PIPELINE_EXECUTIONS",
            "Setting Snowflake context (role, warehouse, database, schema)..."
        )
        
        cursor.execute(f"USE ROLE {pipeline_tracking_role_name}")
        log.info("FETCH_PENDING_PIPELINE_EXECUTIONS", f"Set role to: {pipeline_tracking_role_name}", role=pipeline_tracking_role_name)
        
        cursor.execute(f"USE WAREHOUSE {pipeline_tracking_warehouse_name}")
        log.info("FETCH_PENDING_PIPELINE_EXECUTIONS", f"Set warehouse to: {pipeline_tracking_warehouse_name}", warehouse=pipeline_tracking_warehouse_name)
        
        cursor.execute(f"USE DATABASE {pipeline_tracking_database_name}")
        log.info("FETCH_PENDING_PIPELINE_EXECUTIONS", f"Set database to: {pipeline_tracking_database_name}", database=pipeline_tracking_database_name)
        
        cursor.execute(f"USE SCHEMA {pipeline_tracking_schema_name}")
        log.info("FETCH_PENDING_PIPELINE_EXECUTIONS", f"Set schema to: {pipeline_tracking_schema_name}", schema=pipeline_tracking_schema_name)
        
        # Prepare query parameters
        query_params: Dict[str, Any] = {
            'table_name': pipeline_tracking_table_name,
            'pending_status': 'PENDING',
            'fetch_limit': max_pending_records_fetch_limit
        }
        
        # SQL template with named parameters (for Snowflake API)
        fetch_pending_sql = """
            SELECT *
            FROM %(table_name)s
            WHERE PIPELINE_STATUS = %(pending_status)s
            ORDER BY QUERY_WINDOW_START_TIMESTAMP ASC
            LIMIT %(fetch_limit)s;
        """
        
        log.info(
            "FETCH_PENDING_PIPELINE_EXECUTIONS",
            f"Executing SELECT statement for pending pipeline executions...",
            fetch_limit=max_pending_records_fetch_limit
        )
        
        # Format and print the query for manual testing
        format_and_print_sql_query(fetch_pending_sql, query_params)
        
        # Execute the SELECT statement with dict_cursor using parameterized query
        dict_cursor.execute(fetch_pending_sql, query_params)
        
        # Get query ID from dict_cursor
        query_id = dict_cursor.sfqid
        
        # Fetch all results as list of dicts (DictCursor does this automatically)
        records: List[Dict[str, Any]] = dict_cursor.fetchall()
        
        records_count = len(records)
        
        log.info(
            "FETCH_PENDING_PIPELINE_EXECUTIONS",
            f"Pending pipeline executions fetch completed successfully.",
            query_id=query_id,
            records_returned=records_count,
            fetch_limit=max_pending_records_fetch_limit
        )
        
        return_object['continue_dag_run'] = True
        return_object['records'] = records
        return_object['records_count'] = records_count
        
        return return_object
    
    except Exception as e:
        # Handle any errors during fetch operation
        
        import traceback
        error_details = traceback.format_exc()
        error_msg_for_email = (
            f"Fetch Pending Pipeline Executions Failed!\n\n"
            f"Error Location: fetch_pending_pipeline_executions\n"
            f"Error Type: {type(e).__name__}\n"
            f"Message: {str(e)}\n\n"
            f"Full Traceback:\n{error_details}"
        )
        
        log.error(
            "FETCH_PENDING_PIPELINE_EXECUTIONS",
            "Failed to fetch pending pipeline executions.",
            exc_info=True,
            error_message=str(e)
        )
        
        return_object['error_message'] = error_msg_for_email
        
        return return_object
    
    finally:
        # Ensure connections are closed
        if dict_cursor is not None:
            try:
                dict_cursor.close()
            except Exception as e:
                log.warning(
                    "FETCH_PENDING_PIPELINE_EXECUTIONS",
                    "Failed to close dict_cursor gracefully.",
                    error_message=str(e)
                )
        
        if cursor is not None:
            try:
                cursor.close()
            except Exception as e:
                log.warning(
                    "FETCH_PENDING_PIPELINE_EXECUTIONS",
                    "Failed to close cursor gracefully.",
                    error_message=str(e)
                )
        
        if conn is not None:
            try:
                conn.close()
            except Exception as e:
                log.warning(
                    "FETCH_PENDING_PIPELINE_EXECUTIONS",
                    "Failed to close connection gracefully.",
                    error_message=str(e)
                )




"""
Function: insert_new_record_into_tracking_table
Location: my_main_project_folder/framework/pipeline_tracking_manager.py
Purpose: Insert a new record into the PIPELINE_EXECUTION_TRACKING table
"""

import traceback
from typing import Dict, Any, List
from snowflake.connector import DictCursor
from utils.custom_logging import CustomChainLogger
from utils.sql_formatter_utility import format_and_print_sql_query
from framework.snowflake_connector import get_snowflake_connection


def insert_new_record_into_tracking_table(
    config: Dict[str, Any],
    record: Dict[str, Any],
    logger: CustomChainLogger
) -> Dict[str, Any]:
    """
    Inserts a new record into the PIPELINE_EXECUTION_TRACKING table.
    
    Args:
        config (Dict[str, Any]): Configuration dictionary containing:
                      - config["sf_con_parms"]: Snowflake connection params
                      - config["pipeline_tracking_details"]: Pipeline tracking details with:
                          - pipeline_tracking_table_name
                          - pipeline_tracking_database_name
                          - pipeline_tracking_schema_name
                          - pipeline_tracking_warehouse_name
                          - pipeline_tracking_role_name
        record (Dict[str, Any]): Record to insert containing columns:
                      - TRACKING_ID
                      - PIPELINE_ID
                      - PIPELINE_NAME
                      - QUERY_WINDOW_START_TIMESTAMP
                      - QUERY_WINDOW_END_TIMESTAMP
                      - WINDOW_INTERVAL
                      - PIPELINE_START_TIMESTAMP
                      - PIPELINE_END_TIMESTAMP (nullable)
                      - PIPELINE_STATUS
                      - PIPELINE_RETRY_ATTEMPT
                      - BACKFILL_REQUEST_ID (nullable)
                      - COMPLETED_PHASES (VARIANT)
                      - PENDING_PHASES (VARIANT)
                      - RUNNING_PHASE
                      - FAILED_PHASE
                      - SKIPPED_PHASES (VARIANT)
                      - CREATED_BY
        logger (CustomChainLogger): Our custom logger for logging operations.
    
    Returns:
        Dict[str, Any]: A dictionary containing:
            - 'continue_dag_run' (bool): Whether the DAG should continue execution
            - 'error_message' (str): Error details if operation failed (None if successful)
            - 'records_inserted' (int): Number of records inserted
            - 'tracking_id' (str): The tracking_id of the inserted record
    """
    
    log = logger.new_frame("insert_new_record_into_tracking_table")
    
    return_object: Dict[str, Any] = {
        'continue_dag_run': False,
        'error_message': None,
        'records_inserted': 0,
        'tracking_id': None
    }
    
    conn = None
    cursor = None
    
    try:
        # Extract Snowflake connection parameters from config
        sf_con_parms = config.get("sf_con_parms")
        pipeline_tracking_details = config.get("pipeline_tracking_details")
        
        # Collect all validation errors
        validation_errors: List[str] = []
        
        if not sf_con_parms:
            validation_errors.append("Missing 'sf_con_parms' in config dictionary")
        
        if not pipeline_tracking_details:
            validation_errors.append("Missing 'pipeline_tracking_details' in config dictionary")
        
        if not record:
            validation_errors.append("Missing 'record' parameter")
        
        # Extract pipeline tracking details with specific names
        pipeline_tracking_table_name = pipeline_tracking_details.get("pipeline_tracking_table_name") if pipeline_tracking_details else None
        pipeline_tracking_database_name = pipeline_tracking_details.get("pipeline_tracking_database_name") if pipeline_tracking_details else None
        pipeline_tracking_schema_name = pipeline_tracking_details.get("pipeline_tracking_schema_name") if pipeline_tracking_details else None
        pipeline_tracking_warehouse_name = pipeline_tracking_details.get("pipeline_tracking_warehouse_name") if pipeline_tracking_details else None
        pipeline_tracking_role_name = pipeline_tracking_details.get("pipeline_tracking_role_name") if pipeline_tracking_details else None
        
        if not pipeline_tracking_table_name:
            validation_errors.append("Missing 'pipeline_tracking_table_name' in pipeline_tracking_details")
        
        if not pipeline_tracking_database_name:
            validation_errors.append("Missing 'pipeline_tracking_database_name' in pipeline_tracking_details")
        
        if not pipeline_tracking_schema_name:
            validation_errors.append("Missing 'pipeline_tracking_schema_name' in pipeline_tracking_details")
        
        if not pipeline_tracking_warehouse_name:
            validation_errors.append("Missing 'pipeline_tracking_warehouse_name' in pipeline_tracking_details")
        
        if not pipeline_tracking_role_name:
            validation_errors.append("Missing 'pipeline_tracking_role_name' in pipeline_tracking_details")
        
        # Validate required fields in record
        required_record_fields = [
            'TRACKING_ID', 'PIPELINE_ID', 'PIPELINE_NAME',
            'QUERY_WINDOW_START_TIMESTAMP', 'QUERY_WINDOW_END_TIMESTAMP',
            'PIPELINE_START_TIMESTAMP', 'PIPELINE_STATUS', 'CREATED_BY'
        ]
        
        for field in required_record_fields:
            if field not in record or record[field] is None:
                validation_errors.append(f"Missing or None value for required field '{field}' in record")
        
        # If any validation errors, collect them and exit
        if validation_errors:
            error_details = "\n".join(validation_errors)
            error_msg_for_email = (
                f"Insert Record Into Tracking Table Failed - Validation Error!\n\n"
                f"Error Location: insert_new_record_into_tracking_table\n"
                f"Error Type: ValidationError\n\n"
                f"Validation Errors:\n{error_details}"
            )
            
            log.error(
                "INSERT_NEW_RECORD_INTO_TRACKING_TABLE",
                "Validation failed.",
                validation_errors=error_details
            )
            
            return_object['error_message'] = error_msg_for_email
            return return_object
        
        log.info(
            "INSERT_NEW_RECORD_INTO_TRACKING_TABLE",
            "Attempting to establish Snowflake connection for inserting record...",
            account=sf_con_parms.get("account"),
            username=sf_con_parms.get("username"),
            database=pipeline_tracking_database_name,
            schema=pipeline_tracking_schema_name,
            warehouse=pipeline_tracking_warehouse_name,
            role=pipeline_tracking_role_name,
            table_name=pipeline_tracking_table_name,
            tracking_id=record.get('TRACKING_ID')
        )
        
        # Get Snowflake connection
        conn_result = get_snowflake_connection(sf_con_parms, log)
        
        if not conn_result['continue_dag_run']:
            log.error(
                "INSERT_NEW_RECORD_INTO_TRACKING_TABLE",
                "Failed to establish Snowflake connection.",
                error_message=conn_result['error_message']
            )
            return_object['error_message'] = conn_result['error_message']
            return return_object
        
        conn = conn_result['conn']
        cursor = conn_result['cursor']
        
        log.info(
            "INSERT_NEW_RECORD_INTO_TRACKING_TABLE",
            "Snowflake connection established successfully."
        )
        
        # Set the role, warehouse, database, and schema
        log.info(
            "INSERT_NEW_RECORD_INTO_TRACKING_TABLE",
            "Setting Snowflake context (role, warehouse, database, schema)..."
        )
        
        cursor.execute(f"USE ROLE {pipeline_tracking_role_name}")
        log.info("INSERT_NEW_RECORD_INTO_TRACKING_TABLE", f"Set role to: {pipeline_tracking_role_name}", role=pipeline_tracking_role_name)
        
        cursor.execute(f"USE WAREHOUSE {pipeline_tracking_warehouse_name}")
        log.info("INSERT_NEW_RECORD_INTO_TRACKING_TABLE", f"Set warehouse to: {pipeline_tracking_warehouse_name}", warehouse=pipeline_tracking_warehouse_name)
        
        cursor.execute(f"USE DATABASE {pipeline_tracking_database_name}")
        log.info("INSERT_NEW_RECORD_INTO_TRACKING_TABLE", f"Set database to: {pipeline_tracking_database_name}", database=pipeline_tracking_database_name)
        
        cursor.execute(f"USE SCHEMA {pipeline_tracking_schema_name}")
        log.info("INSERT_NEW_RECORD_INTO_TRACKING_TABLE", f"Set schema to: {pipeline_tracking_schema_name}", schema=pipeline_tracking_schema_name)
        
        # Prepare query parameters - extract values from record
        query_params: Dict[str, Any] = {
            'table_name': pipeline_tracking_table_name,
            'tracking_id': record.get('TRACKING_ID'),
            'pipeline_id': record.get('PIPELINE_ID'),
            'pipeline_name': record.get('PIPELINE_NAME'),
            'query_window_start_timestamp': record.get('QUERY_WINDOW_START_TIMESTAMP'),
            'query_window_end_timestamp': record.get('QUERY_WINDOW_END_TIMESTAMP'),
            'window_interval': record.get('WINDOW_INTERVAL'),
            'pipeline_start_timestamp': record.get('PIPELINE_START_TIMESTAMP'),
            'pipeline_end_timestamp': record.get('PIPELINE_END_TIMESTAMP'),
            'pipeline_status': record.get('PIPELINE_STATUS'),
            'pipeline_retry_attempt': record.get('PIPELINE_RETRY_ATTEMPT', 0),
            'backfill_request_id': record.get('BACKFILL_REQUEST_ID'),
            'completed_phases': record.get('COMPLETED_PHASES'),
            'pending_phases': record.get('PENDING_PHASES'),
            'running_phase': record.get('RUNNING_PHASE'),
            'failed_phase': record.get('FAILED_PHASE'),
            'skipped_phases': record.get('SKIPPED_PHASES'),
            'created_by': record.get('CREATED_BY')
        }
        
        # SQL template with named parameters (for Snowflake API)
        insert_sql = """
INSERT INTO %(table_name)s (
    TRACKING_ID,
    PIPELINE_ID,
    PIPELINE_NAME,
    QUERY_WINDOW_START_TIMESTAMP,
    QUERY_WINDOW_END_TIMESTAMP,
    WINDOW_INTERVAL,
    PIPELINE_START_TIMESTAMP,
    PIPELINE_END_TIMESTAMP,
    PIPELINE_STATUS,
    PIPELINE_RETRY_ATTEMPT,
    BACKFILL_REQUEST_ID,
    COMPLETED_PHASES,
    PENDING_PHASES,
    RUNNING_PHASE,
    FAILED_PHASE,
    SKIPPED_PHASES,
    CREATED_BY
)
VALUES (
    %(tracking_id)s,
    %(pipeline_id)s,
    %(pipeline_name)s,
    %(query_window_start_timestamp)s,
    %(query_window_end_timestamp)s,
    %(window_interval)s,
    %(pipeline_start_timestamp)s,
    %(pipeline_end_timestamp)s,
    %(pipeline_status)s,
    %(pipeline_retry_attempt)s,
    %(backfill_request_id)s,
    %(completed_phases)s,
    %(pending_phases)s,
    %(running_phase)s,
    %(failed_phase)s,
    %(skipped_phases)s,
    %(created_by)s
);
        """
        
        log.info(
            "INSERT_NEW_RECORD_INTO_TRACKING_TABLE",
            f"Executing INSERT statement for tracking record...",
            tracking_id=record.get('TRACKING_ID')
        )
        
        # Format and print the query for manual testing
        format_and_print_sql_query(insert_sql, query_params)
        
        # Execute the INSERT statement
        cursor.execute(insert_sql, query_params)
        
        # Get query ID from cursor
        query_id = cursor.sfqid
        rows_affected = cursor.rowcount
        
        log.info(
            "INSERT_NEW_RECORD_INTO_TRACKING_TABLE",
            f"Tracking record inserted successfully.",
            query_id=query_id,
            rows_affected=rows_affected,
            tracking_id=record.get('TRACKING_ID')
        )
        
        return_object['continue_dag_run'] = True
        return_object['records_inserted'] = rows_affected
        return_object['tracking_id'] = record.get('TRACKING_ID')
        
        return return_object
    
    except Exception as e:
        # Handle any errors during insert operation
        
        error_details = traceback.format_exc()
        error_msg_for_email = (
            f"Insert Record Into Tracking Table Failed!\n\n"
            f"Error Location: insert_new_record_into_tracking_table\n"
            f"Error Type: {type(e).__name__}\n"
            f"Message: {str(e)}\n\n"
            f"Full Traceback:\n{error_details}"
        )
        
        log.error(
            "INSERT_NEW_RECORD_INTO_TRACKING_TABLE",
            "Failed to insert record into tracking table.",
            exc_info=True,
            error_message=str(e),
            tracking_id=record.get('TRACKING_ID')
        )
        
        return_object['error_message'] = error_msg_for_email
        
        return return_object
    
    finally:
        # Ensure connections are closed
        if cursor is not None:
            try:
                cursor.close()
            except Exception as e:
                log.warning(
                    "INSERT_NEW_RECORD_INTO_TRACKING_TABLE",
                    "Failed to close cursor gracefully.",
                    error_message=str(e)
                )
        
        if conn is not None:
            try:
                conn.close()
            except Exception as e:
                log.warning(
                    "INSERT_NEW_RECORD_INTO_TRACKING_TABLE",
                    "Failed to close connection gracefully.",
                    error_message=str(e)
                )




"""
Function: update_record_in_tracking_table
Location: my_main_project_folder/framework/pipeline_tracking_manager.py
Purpose: Update an existing record in the PIPELINE_EXECUTION_TRACKING table
"""

import traceback
from typing import Dict, Any, List
from utils.custom_logging import CustomChainLogger
from utils.sql_formatter_utility import format_and_print_sql_query
from framework.snowflake_connector import get_snowflake_connection


def update_record_in_tracking_table(
    config: Dict[str, Any],
    old_record: Dict[str, Any],
    new_record: Dict[str, Any],
    logger: CustomChainLogger
) -> Dict[str, Any]:
    """
    Updates an existing record in the PIPELINE_EXECUTION_TRACKING table.
    
    Updates the record identified by old_record (TRACKING_ID and PIPELINE_RETRY_ATTEMPT)
    with values from new_record.
    
    Args:
        config (Dict[str, Any]): Configuration dictionary containing:
                      - config["sf_con_parms"]: Snowflake connection params
                      - config["pipeline_tracking_details"]: Pipeline tracking details with:
                          - pipeline_tracking_table_name
                          - pipeline_tracking_database_name
                          - pipeline_tracking_schema_name
                          - pipeline_tracking_warehouse_name
                          - pipeline_tracking_role_name
        old_record (Dict[str, Any]): Existing record with keys:
                      - TRACKING_ID (required - used for WHERE clause)
                      - PIPELINE_RETRY_ATTEMPT (required - used for WHERE clause)
        new_record (Dict[str, Any]): New values to update, can contain any columns:
                      - PIPELINE_NAME
                      - QUERY_WINDOW_START_TIMESTAMP
                      - QUERY_WINDOW_END_TIMESTAMP
                      - WINDOW_INTERVAL
                      - PIPELINE_START_TIMESTAMP
                      - PIPELINE_END_TIMESTAMP
                      - PIPELINE_STATUS
                      - PIPELINE_RETRY_ATTEMPT
                      - BACKFILL_REQUEST_ID
                      - COMPLETED_PHASES
                      - PENDING_PHASES
                      - RUNNING_PHASE
                      - FAILED_PHASE
                      - SKIPPED_PHASES
        logger (CustomChainLogger): Our custom logger for logging operations.
    
    Returns:
        Dict[str, Any]: A dictionary containing:
            - 'continue_dag_run' (bool): Whether the DAG should continue execution
            - 'error_message' (str): Error details if operation failed (None if successful)
            - 'records_updated' (int): Number of records updated
            - 'tracking_id' (str): The tracking_id of the updated record
    """
    
    log = logger.new_frame("update_record_in_tracking_table")
    
    return_object: Dict[str, Any] = {
        'continue_dag_run': False,
        'error_message': None,
        'records_updated': 0,
        'tracking_id': None
    }
    
    conn = None
    cursor = None
    
    try:
        # Extract Snowflake connection parameters from config
        sf_con_parms = config.get("sf_con_parms")
        pipeline_tracking_details = config.get("pipeline_tracking_details")
        
        # Collect all validation errors
        validation_errors: List[str] = []
        
        if not sf_con_parms:
            validation_errors.append("Missing 'sf_con_parms' in config dictionary")
        
        if not pipeline_tracking_details:
            validation_errors.append("Missing 'pipeline_tracking_details' in config dictionary")
        
        if not old_record:
            validation_errors.append("Missing 'old_record' parameter")
        
        if not new_record:
            validation_errors.append("Missing 'new_record' parameter")
        
        # Extract pipeline tracking details with specific names
        pipeline_tracking_table_name = pipeline_tracking_details.get("pipeline_tracking_table_name") if pipeline_tracking_details else None
        pipeline_tracking_database_name = pipeline_tracking_details.get("pipeline_tracking_database_name") if pipeline_tracking_details else None
        pipeline_tracking_schema_name = pipeline_tracking_details.get("pipeline_tracking_schema_name") if pipeline_tracking_details else None
        pipeline_tracking_warehouse_name = pipeline_tracking_details.get("pipeline_tracking_warehouse_name") if pipeline_tracking_details else None
        pipeline_tracking_role_name = pipeline_tracking_details.get("pipeline_tracking_role_name") if pipeline_tracking_details else None
        
        if not pipeline_tracking_table_name:
            validation_errors.append("Missing 'pipeline_tracking_table_name' in pipeline_tracking_details")
        
        if not pipeline_tracking_database_name:
            validation_errors.append("Missing 'pipeline_tracking_database_name' in pipeline_tracking_details")
        
        if not pipeline_tracking_schema_name:
            validation_errors.append("Missing 'pipeline_tracking_schema_name' in pipeline_tracking_details")
        
        if not pipeline_tracking_warehouse_name:
            validation_errors.append("Missing 'pipeline_tracking_warehouse_name' in pipeline_tracking_details")
        
        if not pipeline_tracking_role_name:
            validation_errors.append("Missing 'pipeline_tracking_role_name' in pipeline_tracking_details")
        
        # If any validation errors, collect them and exit
        if validation_errors:
            error_details = "\n".join(validation_errors)
            error_msg_for_email = (
                f"Update Record In Tracking Table Failed - Validation Error!\n\n"
                f"Error Location: update_record_in_tracking_table\n"
                f"Error Type: ValidationError\n\n"
                f"Validation Errors:\n{error_details}"
            )
            
            log.error(
                "UPDATE_RECORD_IN_TRACKING_TABLE",
                "Validation failed.",
                validation_errors=error_details
            )
            
            return_object['error_message'] = error_msg_for_email
            return return_object
        
        log.info(
            "UPDATE_RECORD_IN_TRACKING_TABLE",
            "Attempting to establish Snowflake connection for updating record...",
            account=sf_con_parms.get("account"),
            username=sf_con_parms.get("username"),
            database=pipeline_tracking_database_name,
            schema=pipeline_tracking_schema_name,
            warehouse=pipeline_tracking_warehouse_name,
            role=pipeline_tracking_role_name,
            table_name=pipeline_tracking_table_name,
            tracking_id=old_record.get('TRACKING_ID'),
            pipeline_retry_attempt=old_record.get('PIPELINE_RETRY_ATTEMPT')
        )
        
        # Get Snowflake connection
        conn_result = get_snowflake_connection(sf_con_parms, log)
        
        if not conn_result['continue_dag_run']:
            log.error(
                "UPDATE_RECORD_IN_TRACKING_TABLE",
                "Failed to establish Snowflake connection.",
                error_message=conn_result['error_message']
            )
            return_object['error_message'] = conn_result['error_message']
            return return_object
        
        conn = conn_result['conn']
        cursor = conn_result['cursor']
        
        log.info(
            "UPDATE_RECORD_IN_TRACKING_TABLE",
            "Snowflake connection established successfully."
        )
        
        # Set the role, warehouse, database, and schema
        log.info(
            "UPDATE_RECORD_IN_TRACKING_TABLE",
            "Setting Snowflake context (role, warehouse, database, schema)..."
        )
        
        cursor.execute(f"USE ROLE {pipeline_tracking_role_name}")
        log.info("UPDATE_RECORD_IN_TRACKING_TABLE", f"Set role to: {pipeline_tracking_role_name}", role=pipeline_tracking_role_name)
        
        cursor.execute(f"USE WAREHOUSE {pipeline_tracking_warehouse_name}")
        log.info("UPDATE_RECORD_IN_TRACKING_TABLE", f"Set warehouse to: {pipeline_tracking_warehouse_name}", warehouse=pipeline_tracking_warehouse_name)
        
        cursor.execute(f"USE DATABASE {pipeline_tracking_database_name}")
        log.info("UPDATE_RECORD_IN_TRACKING_TABLE", f"Set database to: {pipeline_tracking_database_name}", database=pipeline_tracking_database_name)
        
        cursor.execute(f"USE SCHEMA {pipeline_tracking_schema_name}")
        log.info("UPDATE_RECORD_IN_TRACKING_TABLE", f"Set schema to: {pipeline_tracking_schema_name}", schema=pipeline_tracking_schema_name)
        
        # Build WHERE clause using all fields from old_record
        where_clauses = []
        query_params: Dict[str, Any] = {
            'table_name': pipeline_tracking_table_name
        }
        
        # Add all old_record fields to WHERE clause
        for column_name, column_value in old_record.items():
            param_name = f"old_{column_name.lower()}"
            where_clauses.append(f"{column_name} = %({param_name})s")
            query_params[param_name] = column_value
        
        # Build SET clause using all fields from new_record
        set_clauses = []
        for column_name, column_value in new_record.items():
            param_name = f"new_{column_name.lower()}"
            set_clauses.append(f"{column_name} = %({param_name})s")
            query_params[param_name] = column_value
        
        # Always update UPDATED_AT
        set_clauses.append("UPDATED_AT = CURRENT_TIMESTAMP()")
        
        # Build the complete UPDATE statement
        where_clause_str = " AND ".join(where_clauses)
        set_clause_str = ", ".join(set_clauses)
        
        # SQL template with named parameters (for Snowflake API)
        update_sql = f"""
UPDATE %(table_name)s
SET {set_clause_str}
WHERE {where_clause_str};
        """
        
        log.info(
            "UPDATE_RECORD_IN_TRACKING_TABLE",
            f"Executing UPDATE statement for tracking record...",
            tracking_id=old_record.get('TRACKING_ID'),
            pipeline_retry_attempt=old_record.get('PIPELINE_RETRY_ATTEMPT')
        )
        
        # Format and print the query for manual testing
        format_and_print_sql_query(update_sql, query_params)
        
        # Execute the UPDATE statement
        cursor.execute(update_sql, query_params)
        
        # Get query ID from cursor
        query_id = cursor.sfqid
        rows_affected = cursor.rowcount
        
        log.info(
            "UPDATE_RECORD_IN_TRACKING_TABLE",
            f"Tracking record updated successfully.",
            query_id=query_id,
            rows_affected=rows_affected,
            tracking_id=old_record.get('TRACKING_ID'),
            pipeline_retry_attempt=old_record.get('PIPELINE_RETRY_ATTEMPT')
        )
        
        return_object['continue_dag_run'] = True
        return_object['records_updated'] = rows_affected
        return_object['tracking_id'] = old_record.get('TRACKING_ID')
        
        return return_object
    
    except Exception as e:
        # Handle any errors during update operation
        
        error_details = traceback.format_exc()
        error_msg_for_email = (
            f"Update Record In Tracking Table Failed!\n\n"
            f"Error Location: update_record_in_tracking_table\n"
            f"Error Type: {type(e).__name__}\n"
            f"Message: {str(e)}\n\n"
            f"Full Traceback:\n{error_details}"
        )
        
        log.error(
            "UPDATE_RECORD_IN_TRACKING_TABLE",
            "Failed to update record in tracking table.",
            exc_info=True,
            error_message=str(e),
            tracking_id=old_record.get('TRACKING_ID'),
            pipeline_retry_attempt=old_record.get('PIPELINE_RETRY_ATTEMPT')
        )
        
        return_object['error_message'] = error_msg_for_email
        
        return return_object
    
    finally:
        # Ensure connections are closed
        if cursor is not None:
            try:
                cursor.close()
            except Exception as e:
                log.warning(
                    "UPDATE_RECORD_IN_TRACKING_TABLE",
                    "Failed to close cursor gracefully.",
                    error_message=str(e)
                )
        
        if conn is not None:
            try:
                conn.close()
            except Exception as e:
                log.warning(
                    "UPDATE_RECORD_IN_TRACKING_TABLE",
                    "Failed to close connection gracefully.",
                    error_message=str(e)
                )















