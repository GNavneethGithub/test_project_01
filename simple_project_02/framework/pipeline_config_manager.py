# my_main_project_folder/framework/pipeline_config_manager.py


"""
Function: create_pipeline_config_table
Location: my_main_project_folder/framework/pipeline_config_manager.py
Purpose: Simplified wrapper for creating PIPELINE_CONFIG table
"""

from typing import Dict, Any
from utils.custom_logging import CustomChainLogger
from framework.generic_table_creator import create_table_generic


def create_pipeline_config_table(
    config: Dict[str, Any],
    logger: CustomChainLogger
) -> Dict[str, Any]:
    """
    Creates the PIPELINE_CONFIG table in Snowflake if it does not already exist.
    
    This is a simplified wrapper around create_table_generic() that handles
    pipeline configuration table creation.
    
    Args:
        config (Dict[str, Any]): Configuration dictionary containing:
                      - config["sf_con_parms"]: Snowflake connection params
                      - config["pipeline_config_details"]: Pipeline config details with:
                          - pipeline_config_table_name
                          - pipeline_config_database_name
                          - pipeline_config_schema_name
                          - pipeline_config_warehouse_name
                          - pipeline_config_role_name
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
    PIPELINE_ID VARCHAR NOT NULL,
    CONFIG_VERSION NUMBER NOT NULL,
    PIPELINE_NAME VARCHAR NOT NULL,
    CONFIG_JSON VARIANT NOT NULL,
    CREATED_AT TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    CREATED_BY VARCHAR NOT NULL,
    VALID_FROM TIMESTAMP_TZ NOT NULL,
    VALID_TO TIMESTAMP_TZ,
    IS_ACTIVE BOOLEAN NOT NULL DEFAULT TRUE,
    CHANGE_REASON VARCHAR,
    ENVIRONMENT VARCHAR,
    PRIMARY KEY (PIPELINE_ID, CONFIG_VERSION)
);
    """
    
    # Extract required parameters from config
    sf_con_parms = config.get("sf_con_parms")
    pipeline_config_details = config.get("pipeline_config_details")
    
    # Normalize table_details keys (remove prefix for generic function)
    table_details: Dict[str, Any] = {}
    if pipeline_config_details:
        table_details = {
            'table_name': pipeline_config_details.get("pipeline_config_table_name"),
            'database_name': pipeline_config_details.get("pipeline_config_database_name"),
            'schema_name': pipeline_config_details.get("pipeline_config_schema_name"),
            'warehouse_name': pipeline_config_details.get("pipeline_config_warehouse_name"),
            'role_name': pipeline_config_details.get("pipeline_config_role_name")
        }
    
    # Call generic table creator
    return create_table_generic(
        sf_con_parms=sf_con_parms,
        table_details=table_details,
        log_keyword="CREATE_PIPELINE_CONFIG_TABLE",
        new_frame_name="create_pipeline_config_table",
        create_table_sql=create_table_sql,
        logger=logger,
        config=config
    )


"""
Function: fetch_only_active_pipeline_config_details
Location: my_main_project_folder/framework/pipeline_config_manager.py
"""

import traceback
from typing import Dict, List, Any
import snowflake.connector
from snowflake.connector import DictCursor
from utils.custom_logging import CustomChainLogger
from utils.sql_formatter_utility import format_and_print_sql_query
from framework.snowflake_connector import get_snowflake_connection


def fetch_only_active_pipeline_config_details(
    config: Dict[str, Any],
    logger: CustomChainLogger
) -> Dict[str, Any]:
    """
    Fetches the active pipeline configuration for a given pipeline_name.
    
    Retrieves the latest version of the pipeline config that:
    - Matches the pipeline_name
    - Has is_active = true
    
    Args:
        config (Dict[str, Any]): Configuration dictionary containing:
                      - config["sf_con_parms"]: Snowflake connection params
                      - config["pipeline_config_details"]: Pipeline config details with:
                          - pipeline_config_table_name
                          - pipeline_config_database_name
                          - pipeline_config_schema_name
                          - pipeline_config_warehouse_name
                          - pipeline_config_role_name
                      - config["pipeline_name"]: Name of the pipeline to fetch
        logger (CustomChainLogger): Our custom logger for logging operations.
    
    Returns:
        Dict[str, Any]: A dictionary containing:
            - 'continue_dag_run' (bool): Whether the DAG should continue execution
            - 'error_message' (str): Error details if operation failed (None if successful)
            - 'records' (List[Dict]): List of dictionaries containing the config records
    """
    
    log = logger.new_frame("fetch_only_active_pipeline_config_details")
    
    return_object: Dict[str, Any] = {
        'continue_dag_run': False,
        'error_message': None,
        'records': []
    }
    
    conn = None
    cursor = None
    dict_cursor = None
    
    try:
        # Extract Snowflake connection parameters from config
        sf_con_parms = config.get("sf_con_parms")
        pipeline_config_details = config.get("pipeline_config_details")
        pipeline_name = config.get("pipeline_name")
        
        # Collect all validation errors
        validation_errors: List[str] = []
        
        if not sf_con_parms:
            validation_errors.append("Missing 'sf_con_parms' in config dictionary")
        
        if not pipeline_config_details:
            validation_errors.append("Missing 'pipeline_config_details' in config dictionary")
        
        if not pipeline_name:
            validation_errors.append("Missing 'pipeline_name' in config dictionary")
        
        # Extract pipeline config details with specific names
        pipeline_config_table_name = pipeline_config_details.get("pipeline_config_table_name") if pipeline_config_details else None
        pipeline_config_database_name = pipeline_config_details.get("pipeline_config_database_name") if pipeline_config_details else None
        pipeline_config_schema_name = pipeline_config_details.get("pipeline_config_schema_name") if pipeline_config_details else None
        pipeline_config_warehouse_name = pipeline_config_details.get("pipeline_config_warehouse_name") if pipeline_config_details else None
        pipeline_config_role_name = pipeline_config_details.get("pipeline_config_role_name") if pipeline_config_details else None
        
        if not pipeline_config_table_name:
            validation_errors.append("Missing 'pipeline_config_table_name' in pipeline_config_details")
        
        if not pipeline_config_database_name:
            validation_errors.append("Missing 'pipeline_config_database_name' in pipeline_config_details")
        
        if not pipeline_config_schema_name:
            validation_errors.append("Missing 'pipeline_config_schema_name' in pipeline_config_details")
        
        if not pipeline_config_warehouse_name:
            validation_errors.append("Missing 'pipeline_config_warehouse_name' in pipeline_config_details")
        
        if not pipeline_config_role_name:
            validation_errors.append("Missing 'pipeline_config_role_name' in pipeline_config_details")
        
        # If any validation errors, collect them and exit
        if validation_errors:
            error_details = "\n".join(validation_errors)
            error_msg_for_email = (
                f"Fetch Pipeline Config Failed - Configuration Validation Error!\n\n"
                f"Error Location: fetch_only_active_pipeline_config_details\n"
                f"Error Type: ConfigurationError\n\n"
                f"Validation Errors:\n{error_details}"
            )
            
            log.error(
                "FETCH_ONLY_ACTIVE_PIPELINE_CONFIG_DETAILS",
                "Configuration validation failed.",
                validation_errors=error_details
            )
            
            return_object['error_message'] = error_msg_for_email
            return return_object
        
        log.info(
            "FETCH_ONLY_ACTIVE_PIPELINE_CONFIG_DETAILS",
            "Attempting to establish Snowflake connection for fetching pipeline config...",
            account=sf_con_parms.get("account"),
            username=sf_con_parms.get("username"),
            pipeline_name=pipeline_name
        )
        
        # Get Snowflake connection
        conn_result = get_snowflake_connection(sf_con_parms, log)
        
        if not conn_result['continue_dag_run']:
            log.error(
                "FETCH_ONLY_ACTIVE_PIPELINE_CONFIG_DETAILS",
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
            "FETCH_ONLY_ACTIVE_PIPELINE_CONFIG_DETAILS",
            "Snowflake connection established successfully."
        )
        
        # Set the role, warehouse, database, and schema
        log.info(
            "FETCH_ONLY_ACTIVE_PIPELINE_CONFIG_DETAILS",
            "Setting Snowflake context (role, warehouse, database, schema)..."
        )
        
        cursor.execute(f"USE ROLE {pipeline_config_role_name}")
        log.info("FETCH_ONLY_ACTIVE_PIPELINE_CONFIG_DETAILS", f"Set role to: {pipeline_config_role_name}", role=pipeline_config_role_name)
        
        cursor.execute(f"USE WAREHOUSE {pipeline_config_warehouse_name}")
        log.info("FETCH_ONLY_ACTIVE_PIPELINE_CONFIG_DETAILS", f"Set warehouse to: {pipeline_config_warehouse_name}", warehouse=pipeline_config_warehouse_name)
        
        cursor.execute(f"USE DATABASE {pipeline_config_database_name}")
        log.info("FETCH_ONLY_ACTIVE_PIPELINE_CONFIG_DETAILS", f"Set database to: {pipeline_config_database_name}", database=pipeline_config_database_name)
        
        cursor.execute(f"USE SCHEMA {pipeline_config_schema_name}")
        log.info("FETCH_ONLY_ACTIVE_PIPELINE_CONFIG_DETAILS", f"Set schema to: {pipeline_config_schema_name}", schema=pipeline_config_schema_name)
        
        # Prepare query parameters
        query_params: Dict[str, Any] = {
            'table_name': pipeline_config_table_name,
            'pipeline_name': pipeline_name
        }
        
        # SQL template with named parameters (for Snowflake API)
        fetch_config_sql = """
            SELECT *
            FROM %(table_name)s
            WHERE PIPELINE_NAME = %(pipeline_name)s
            AND IS_ACTIVE = TRUE
            ORDER BY CONFIG_VERSION DESC
            LIMIT 1;
        """
        
        log.info(
            "FETCH_ONLY_ACTIVE_PIPELINE_CONFIG_DETAILS",
            f"Executing SELECT statement for pipeline config...",
            pipeline_name=pipeline_name
        )
        
        # Format and print the query for manual testing
        format_and_print_sql_query(fetch_config_sql, query_params)
        
        # Execute the SELECT statement with dict_cursor using parameterized query
        dict_cursor.execute(fetch_config_sql, query_params)
        
        # Get query ID from dict_cursor
        query_id = dict_cursor.sfqid
        
        # Fetch all results as list of dicts (DictCursor does this automatically)
        records: List[Dict[str, Any]] = dict_cursor.fetchall()
        
        rows_returned = len(records)
        
        log.info(
            "FETCH_ONLY_ACTIVE_PIPELINE_CONFIG_DETAILS",
            f"Pipeline config fetch completed successfully.",
            pipeline_name=pipeline_name,
            query_id=query_id,
            rows_returned=rows_returned
        )
        
        return_object['continue_dag_run'] = True
        return_object['records'] = records
        
        return return_object
    
    except Exception as e:
        # Handle any errors during fetch operation
        
        error_details = traceback.format_exc()
        error_msg_for_email = (
            f"Fetch Pipeline Config Failed!\n\n"
            f"Error Location: fetch_only_active_pipeline_config_details\n"
            f"Error Type: {type(e).__name__}\n"
            f"Message: {str(e)}\n\n"
            f"Full Traceback:\n{error_details}"
        )
        
        log.error(
            "FETCH_ONLY_ACTIVE_PIPELINE_CONFIG_DETAILS",
            "Failed to fetch pipeline config.",
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
                    "FETCH_ONLY_ACTIVE_PIPELINE_CONFIG_DETAILS",
                    "Failed to close dict_cursor gracefully.",
                    error_message=str(e)
                )
        
        if cursor is not None:
            try:
                cursor.close()
            except Exception as e:
                log.warning(
                    "FETCH_ONLY_ACTIVE_PIPELINE_CONFIG_DETAILS",
                    "Failed to close cursor gracefully.",
                    error_message=str(e)
                )
        
        if conn is not None:
            try:
                conn.close()
            except Exception as e:
                log.warning(
                    "FETCH_ONLY_ACTIVE_PIPELINE_CONFIG_DETAILS",
                    "Failed to close connection gracefully.",
                    error_message=str(e)
                )




"""
Function: validate_data_fetch_request_timestamp
Location: my_main_project_folder/framework/pipeline_config_manager.py
Purpose: Validate if data fetch request timestamp is within valid_from and valid_to bounds
"""

import traceback
from datetime import datetime
from typing import Dict, Any
from snowflake.connector import DictCursor
from utils.custom_logging import CustomChainLogger
from utils.sql_formatter_utility import format_and_print_sql_query
from framework.snowflake_connector import get_snowflake_connection


def validate_data_fetch_request_timestamp(
    config: Dict[str, Any],
    logger: CustomChainLogger,
    data_fetch_request_timestamp: datetime
) -> Dict[str, Any]:
    """
    Validates if the given data fetch request timestamp is within valid_from and valid_to bounds.
    
    Checks if the provided timestamp falls within the valid_from and valid_to range of the
    active pipeline configuration. This ensures that data fetch requests are made during
    the allowed time window.
    
    Args:
        config (Dict[str, Any]): Configuration dictionary containing:
                      - config["sf_con_parms"]: Snowflake connection params
                      - config["pipeline_config_details"]: Pipeline config details with:
                          - pipeline_config_table_name
                          - pipeline_config_database_name
                          - pipeline_config_schema_name
                          - pipeline_config_warehouse_name
                          - pipeline_config_role_name
                      - config["pipeline_name"]: Name of the pipeline
        logger (CustomChainLogger): Our custom logger for logging operations.
        data_fetch_request_timestamp (datetime): The timestamp to validate (with timezone info).
    
    Returns:
        Dict[str, Any]: A dictionary containing:
            - 'continue_dag_run' (bool): Whether the DAG should continue execution
            - 'error_message' (str): Error details if operation failed (None if successful)
            - 'request_within_accepted_bounds' (bool): True if timestamp is within bounds, False otherwise
    """
    
    log = logger.new_frame("validate_data_fetch_request_timestamp")
    
    return_object: Dict[str, Any] = {
        'continue_dag_run': False,
        'error_message': None,
        'request_within_accepted_bounds': False
    }
    
    conn = None
    cursor = None
    dict_cursor = None
    
    try:
        # Extract Snowflake connection parameters from config
        sf_con_parms = config.get("sf_con_parms")
        pipeline_config_details = config.get("pipeline_config_details")
        pipeline_name = config.get("pipeline_name")
        
        # Collect all validation errors
        validation_errors: list[str] = []
        
        if not sf_con_parms:
            validation_errors.append("Missing 'sf_con_parms' in config dictionary")
        
        if not pipeline_config_details:
            validation_errors.append("Missing 'pipeline_config_details' in config dictionary")
        
        if not pipeline_name:
            validation_errors.append("Missing 'pipeline_name' in config dictionary")
        
        if not data_fetch_request_timestamp:
            validation_errors.append("Missing 'data_fetch_request_timestamp' parameter")
        
        # Extract pipeline config details with specific names
        pipeline_config_table_name = pipeline_config_details.get("pipeline_config_table_name") if pipeline_config_details else None
        pipeline_config_database_name = pipeline_config_details.get("pipeline_config_database_name") if pipeline_config_details else None
        pipeline_config_schema_name = pipeline_config_details.get("pipeline_config_schema_name") if pipeline_config_details else None
        pipeline_config_warehouse_name = pipeline_config_details.get("pipeline_config_warehouse_name") if pipeline_config_details else None
        pipeline_config_role_name = pipeline_config_details.get("pipeline_config_role_name") if pipeline_config_details else None
        
        if not pipeline_config_table_name:
            validation_errors.append("Missing 'pipeline_config_table_name' in pipeline_config_details")
        
        if not pipeline_config_database_name:
            validation_errors.append("Missing 'pipeline_config_database_name' in pipeline_config_details")
        
        if not pipeline_config_schema_name:
            validation_errors.append("Missing 'pipeline_config_schema_name' in pipeline_config_details")
        
        if not pipeline_config_warehouse_name:
            validation_errors.append("Missing 'pipeline_config_warehouse_name' in pipeline_config_details")
        
        if not pipeline_config_role_name:
            validation_errors.append("Missing 'pipeline_config_role_name' in pipeline_config_details")
        
        # If any validation errors, collect them and exit
        if validation_errors:
            error_details = "\n".join(validation_errors)
            error_msg_for_email = (
                f"Validate Data Fetch Request Timestamp Failed - Configuration Validation Error!\n\n"
                f"Error Location: validate_data_fetch_request_timestamp\n"
                f"Error Type: ConfigurationError\n\n"
                f"Validation Errors:\n{error_details}"
            )
            
            log.error(
                "VALIDATE_DATA_FETCH_REQUEST_TIMESTAMP",
                "Configuration validation failed.",
                validation_errors=error_details
            )
            
            return_object['error_message'] = error_msg_for_email
            return return_object
        
        log.info(
            "VALIDATE_DATA_FETCH_REQUEST_TIMESTAMP",
            "Attempting to establish Snowflake connection for validating timestamp...",
            account=sf_con_parms.get("account"),
            username=sf_con_parms.get("username"),
            pipeline_name=pipeline_name,
            data_fetch_request_timestamp=str(data_fetch_request_timestamp)
        )
        
        # Get Snowflake connection
        conn_result = get_snowflake_connection(sf_con_parms, log)
        
        if not conn_result['continue_dag_run']:
            log.error(
                "VALIDATE_DATA_FETCH_REQUEST_TIMESTAMP",
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
            "VALIDATE_DATA_FETCH_REQUEST_TIMESTAMP",
            "Snowflake connection established successfully."
        )
        
        # Set the role, warehouse, database, and schema
        log.info(
            "VALIDATE_DATA_FETCH_REQUEST_TIMESTAMP",
            "Setting Snowflake context (role, warehouse, database, schema)..."
        )
        
        cursor.execute(f"USE ROLE {pipeline_config_role_name}")
        log.info("VALIDATE_DATA_FETCH_REQUEST_TIMESTAMP", f"Set role to: {pipeline_config_role_name}", role=pipeline_config_role_name)
        
        cursor.execute(f"USE WAREHOUSE {pipeline_config_warehouse_name}")
        log.info("VALIDATE_DATA_FETCH_REQUEST_TIMESTAMP", f"Set warehouse to: {pipeline_config_warehouse_name}", warehouse=pipeline_config_warehouse_name)
        
        cursor.execute(f"USE DATABASE {pipeline_config_database_name}")
        log.info("VALIDATE_DATA_FETCH_REQUEST_TIMESTAMP", f"Set database to: {pipeline_config_database_name}", database=pipeline_config_database_name)
        
        cursor.execute(f"USE SCHEMA {pipeline_config_schema_name}")
        log.info("VALIDATE_DATA_FETCH_REQUEST_TIMESTAMP", f"Set schema to: {pipeline_config_schema_name}", schema=pipeline_config_schema_name)
        
        # Prepare query parameters
        query_params: Dict[str, Any] = {
            'table_name': pipeline_config_table_name,
            'pipeline_name': pipeline_name,
            'request_timestamp': data_fetch_request_timestamp
        }
        
        # SQL template with named parameters (for Snowflake API)
        validate_timestamp_sql = """
            SELECT COUNT(*) AS VALID_COUNT
            FROM %(table_name)s
            WHERE PIPELINE_NAME = %(pipeline_name)s
            AND IS_ACTIVE = TRUE
            AND VALID_FROM <= %(request_timestamp)s
            AND (VALID_TO IS NULL OR VALID_TO >= %(request_timestamp)s);
        """
        
        log.info(
            "VALIDATE_DATA_FETCH_REQUEST_TIMESTAMP",
            f"Executing validation query for timestamp bounds...",
            pipeline_name=pipeline_name,
            data_fetch_request_timestamp=str(data_fetch_request_timestamp)
        )
        
        # Format and print the query for manual testing
        format_and_print_sql_query(validate_timestamp_sql, query_params)
        
        # Execute the SELECT statement with dict_cursor using parameterized query
        dict_cursor.execute(validate_timestamp_sql, query_params)
        
        # Get query ID from dict_cursor
        query_id = dict_cursor.sfqid
        
        # Fetch result as list of dicts (DictCursor does this automatically)
        records: list[Dict[str, Any]] = dict_cursor.fetchall()
        
        rows_returned = len(records)
        
        # Check if timestamp is within bounds
        if records and records[0]['VALID_COUNT'] > 0:
            request_within_bounds = True
        else:
            request_within_bounds = False
        
        log.info(
            "VALIDATE_DATA_FETCH_REQUEST_TIMESTAMP",
            f"Timestamp validation completed successfully.",
            pipeline_name=pipeline_name,
            query_id=query_id,
            request_within_accepted_bounds=request_within_bounds
        )
        
        return_object['continue_dag_run'] = True
        return_object['request_within_accepted_bounds'] = request_within_bounds
        
        return return_object
    
    except Exception as e:
        # Handle any errors during validation
        
        error_details = traceback.format_exc()
        error_msg_for_email = (
            f"Validate Data Fetch Request Timestamp Failed!\n\n"
            f"Error Location: validate_data_fetch_request_timestamp\n"
            f"Error Type: {type(e).__name__}\n"
            f"Message: {str(e)}\n\n"
            f"Full Traceback:\n{error_details}"
        )
        
        log.error(
            "VALIDATE_DATA_FETCH_REQUEST_TIMESTAMP",
            "Failed to validate timestamp.",
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
                    "VALIDATE_DATA_FETCH_REQUEST_TIMESTAMP",
                    "Failed to close dict_cursor gracefully.",
                    error_message=str(e)
                )
        
        if cursor is not None:
            try:
                cursor.close()
            except Exception as e:
                log.warning(
                    "VALIDATE_DATA_FETCH_REQUEST_TIMESTAMP",
                    "Failed to close cursor gracefully.",
                    error_message=str(e)
                )
        
        if conn is not None:
            try:
                conn.close()
            except Exception as e:
                log.warning(
                    "VALIDATE_DATA_FETCH_REQUEST_TIMESTAMP",
                    "Failed to close connection gracefully.",
                    error_message=str(e)
                )





























