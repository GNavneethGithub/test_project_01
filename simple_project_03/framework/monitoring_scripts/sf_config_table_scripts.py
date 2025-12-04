# my_main_project_folder/framework/monitoring_scripts/sf_config_table_scripts.py

import json
import traceback
from typing import Dict, Any, List, Optional
from snowflake.connector import DictCursor
from utils.custom_logging import CustomChainLogger, setup_logger
from utils.sql_formatter_utility import format_and_print_sql_query
from framework.monitoring_scripts.snowflake_connector import get_snowflake_connection


def create_pipeline_config_details_table_if_not_exists(
    config: Dict[str, Any],
    logger: CustomChainLogger,
    query_tag: str = None
) -> Dict[str, Any]:
    """
    Creates the PIPELINE_CONFIG_DETAILS table in Snowflake if it doesn't exist.
    
    This table stores static configuration data for data pipelines including:
    - Pipeline identification and versioning
    - Source, stage, and target configurations (stored as VARIANT/JSON)
    - Ownership and metadata information
    - Active/inactive status and expiry timestamps
    
    Args:
        config (Dict[str, Any]): Configuration dictionary containing:
            - 'sf_config_params': Dict with Snowflake connection parameters
            - 'pipeline_config_details': Dict with table configuration
        logger (CustomChainLogger): Custom logger instance for tracking function execution.
        query_tag (str, optional): QUERY_TAG for session tracking.
    
    Returns:
        Dict[str, Any]: A dictionary containing:
            - 'continue_dag_run' (bool): True if operation succeeded, False otherwise.
            - 'error_message' (Optional[str]): Error message if operation failed, None if successful.
            - 'query_id' (Optional[str]): Snowflake query ID for the CREATE TABLE statement.
            - 'table_name' (str): Full qualified table name (DATABASE.SCHEMA.TABLE).
            - 'table_exists' (bool): True if table was created or already exists.
    """
    log_keyword = "PIPELINE_CONFIG_DETAILS_TABLE_CREATION"    
    log = logger.new_frame(log_keyword)

    # Initialize variables that might not get assigned if early exception occurs
    query_id = None
    conn = None
    cursor = None
    database_name = None
    schema_name = None
    table_name = None
    three_part_name = "UNKNOWN.UNKNOWN.UNKNOWN"
    
    # Initialize return object
    return_object = {
        'continue_dag_run': False,
        'error_message': None,
        'query_id': None,
        'table_name': three_part_name,
        'table_exists': False
    }
    
    try:
        log.info(
            log_keyword,
            "Starting PIPELINE_CONFIG_DETAILS table creation process."
        )
        
        # Extract configuration parameters
        sf_con_parms = config.get('sf_config_params', {})
        pipeline_config_details = config.get('pipeline_config_details', {})
        
        database_name = pipeline_config_details.get('database_name', 'UNKNOWN_DB')
        schema_name = pipeline_config_details.get('schema_name', 'UNKNOWN_SCHEMA')
        table_name = pipeline_config_details.get('table_name', 'UNKNOWN_TABLE')
        
        # Build three-part name
        three_part_name = f"{database_name}.{schema_name}.{table_name}"
        return_object['table_name'] = three_part_name
        
        log.info(
            log_keyword,
            "Configuration extracted.",
            database=database_name,
            schema=schema_name,
            table=table_name,
            full_table_name=three_part_name,
            query_tag=query_tag
        )
        
        # Step 1: Get Snowflake connection
        log.info(
            log_keyword,
            "Attempting to connect to Snowflake."
        )
        
        connection_result = get_snowflake_connection(sf_con_parms, log, QUERY_TAG=query_tag)
        
        if not connection_result['continue_dag_run']:
            log.error(
                log_keyword,
                "Failed to establish Snowflake connection.",
                error_message=connection_result['error_message']
            )
            return_object['error_message'] = connection_result['error_message']
            return return_object
        
        conn = connection_result['conn']
        cursor = connection_result['cursor']
        
        log.info(
            log_keyword,
            "Successfully connected to Snowflake."
        )
        
        # Step 2: Create the table 
        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {three_part_name} (
                
                ROW_ID NUMBER AUTOINCREMENT START 1 INCREMENT 1,
                
                PIPELINE_NAME VARCHAR(1000) DEFAULT 'unknown_pipeline_name',
                CONFIG_NAME VARCHAR(1000) DEFAULT 'unknown_configuration_name',
                CONFIG_VERSION INTEGER DEFAULT 1,
        
                
                PIPELINE_DESC VARCHAR(5000) DEFAULT NULL,

                NUMBER_OF_STAGES INTEGER DEFAULT 0,
                CONFIG VARIANT DEFAULT NULL,
                IS_ACTIVE BOOLEAN DEFAULT TRUE,
                SOURCE_MIN_REQUESTABLE_TIMESTAMP TIMESTAMP_TZ DEFAULT NULL,
                SOURCE_MAX_REQUESTABLE_TIMESTAMP TIMESTAMP_TZ DEFAULT NULL,

                OWNER_LIST VARIANT DEFAULT NULL,
                PIPELINE_RUN_VALID_FROM_TIMESTAMP TIMESTAMP_TZ DEFAULT NULL,                
                PIPELINE_RUN_VALID_TILL_TIMESTAMP TIMESTAMP_TZ DEFAULT NULL,
                CREATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
                UPDATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
                UPDATED_BY VARCHAR(1000) DEFAULT NULL,
                VALID_FROM_TIMESTAMP TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
                VALID_TO_TIMESTAMP TIMESTAMP_TZ DEFAULT NULL
            )
        """
        log.info(
            log_keyword,
            "Executing CREATE TABLE IF NOT EXISTS statement."
        )
        
        # Print formatted SQL for manual testing/verification
        format_and_print_sql_query(create_table_sql, {})
        
        cursor.execute(create_table_sql)
        
        # Capture the query ID
        query_id = cursor.sfqid
        return_object['query_id'] = query_id
        
        log.info(
            log_keyword,
            "Table created successfully or already exists.",
            full_table_name=three_part_name,
            query_id=query_id
        )
        
        # Set success flags
        return_object['table_exists'] = True
        return_object['continue_dag_run'] = True
        
        return return_object
        
    except Exception as e:
        error_details = traceback.format_exc()
        error_msg_for_email = (
            f"Table Creation Failed!\n\n"
            f"Error Location: create_pipeline_config_details_table_if_not_exists\n"
            f"Error Type: {type(e).__name__}\n"
            f"Database: {database_name if database_name else 'N/A'}\n"
            f"Schema: {schema_name if schema_name else 'N/A'}\n"
            f"Table: {table_name if table_name else 'N/A'}\n"
            f"Full Table Name: {three_part_name}\n"
            f"Query ID: {query_id if query_id else 'N/A'}\n\n"
            f"Error Message: {str(e)}\n\n"
            f"Full Traceback:\n{error_details}"
        )
        
        log.error(
            log_keyword,
            "An error occurred during table creation.",
            exc_info=True,
            database=database_name if database_name else 'N/A',
            schema=schema_name if schema_name else 'N/A',
            table=table_name if table_name else 'N/A',
            error_message=str(e),
            query_id=query_id if query_id else 'N/A'
        )
        
        return_object['error_message'] = error_msg_for_email
        return_object['continue_dag_run'] = False
        return return_object
        
    finally:
        # Clean up resources
        if cursor:
            try:
                cursor.close()
                log.info(
                    log_keyword,
                    "Cursor closed successfully."
                )
            except Exception as e:
                log.error(
                    log_keyword,
                    "Error closing cursor.",
                    error_message=str(e)
                )
        
        if conn:
            try:
                conn.close()
                log.info(
                    log_keyword,
                    "Snowflake connection closed successfully."
                )
            except Exception as e:
                log.error(
                    log_keyword,
                    "Error closing connection.",
                    error_message=str(e)
                )


def fetch_pipeline_config_details(
    config: Dict[str, Any],
    logger: CustomChainLogger,
    query_tag: str = None
) -> Dict[str, Any]:
    """
    Fetches pipeline configuration records from PIPELINE_CONFIG_DETAILS table
    based on PIPELINE_NAME, CONFIG_NAME, and CONFIG_VERSION.
    
    Args:
        config (Dict[str, Any]): Configuration dictionary containing:
            - 'sf_config_params': Dict with Snowflake connection parameters
            - 'pipeline_config_details': Dict with table configuration
            - 'fetch_criteria': Dict with search criteria
        logger (CustomChainLogger): Custom logger instance for tracking function execution.
        query_tag (str, optional): QUERY_TAG for session tracking.
    
    Returns:
        Dict[str, Any]: A dictionary containing:
            - 'continue_dag_run' (bool): True if query succeeded, False otherwise.
            - 'error_message' (Optional[str]): Error message if operation failed, None if successful.
            - 'query_id' (Optional[str]): Snowflake query ID for the SELECT statement.
            - 'records' (list): List of dict records found, empty list [] if no records.
    """
    log_keyword = "FETCH_PIPELINE_CONFIG_DETAILS"
    log = logger.new_frame(log_keyword)
    
    # Initialize variables that might not get assigned if early exception occurs
    query_id = None
    conn = None
    cursor = None
    database_name = None
    schema_name = None
    table_name = None
    three_part_name = "UNKNOWN.UNKNOWN.UNKNOWN"
    pipeline_name = None
    config_name = None
    config_version = None
    records = []
    
    # Initialize return object
    return_object = {
        'continue_dag_run': False,
        'error_message': None,
        'query_id': None,
        'records': []
    }
    
    try:
        log.info(
            log_keyword,
            "Starting fetch operation for pipeline configuration details."
        )
        
        # Extract configuration parameters
        sf_con_parms = config.get('sf_config_params', {})
        pipeline_config_details = config.get('pipeline_config_details', {})
        fetch_criteria = config.get('fetch_criteria', {})
        
        database_name = pipeline_config_details.get('database_name', 'UNKNOWN_DB')
        schema_name = pipeline_config_details.get('schema_name', 'UNKNOWN_SCHEMA')
        table_name = pipeline_config_details.get('table_name', 'UNKNOWN_TABLE')
        
        # Build three-part name
        three_part_name = f"{database_name}.{schema_name}.{table_name}"
        
        # Extract fetch criteria
        pipeline_name = fetch_criteria.get('PIPELINE_NAME')
        config_name = fetch_criteria.get('CONFIG_NAME')
        config_version = fetch_criteria.get('CONFIG_VERSION')
        
        log.info(
            log_keyword,
            "Configuration extracted.",
            database=database_name,
            schema=schema_name,
            table=table_name,
            full_table_name=three_part_name,
            pipeline_name=pipeline_name,
            config_name=config_name,
            config_version=config_version,
            query_tag=query_tag
        )
        
        # Step 1: Get Snowflake connection
        log.info(
            log_keyword,
            "Attempting to connect to Snowflake."
        )
        
        connection_result = get_snowflake_connection(sf_con_parms, log, QUERY_TAG=query_tag)
        
        if not connection_result['continue_dag_run']:
            log.error(
                log_keyword,
                "Failed to establish Snowflake connection.",
                error_message=connection_result['error_message']
            )
            return_object['error_message'] = connection_result['error_message']
            return return_object
        
        conn = connection_result['conn']
        
        # Create cursor with DictCursor to get results as dictionaries
        cursor = conn.cursor(DictCursor)
        
        log.info(
            log_keyword,
            "Successfully connected to Snowflake with DictCursor."
        )
        
        # Step 2: Build and execute SELECT query
        select_sql = f"""
            WITH FETCH_CRITERIA AS (
                    SELECT *
                    FROM {three_part_name}
                    WHERE PIPELINE_NAME = %(pipeline_name)s AND CONFIG_NAME = %(config_name)s
                )
            SELECT * FROM FETCH_CRITERIA ORDER BY CONFIG_VERSION DESC, ROW_ID DESC LIMIT 1;
        """
        
        query_params = {
            "pipeline_name": pipeline_name,
            "config_name": config_name
        }
        
        log.info(
            log_keyword,
            "Executing SELECT query."
        )
        
        # Print formatted SQL for manual testing/verification
        format_and_print_sql_query(select_sql, query_params)
        
        cursor.execute(select_sql, query_params)
        
        # Capture the query ID
        query_id = cursor.sfqid
        return_object['query_id'] = query_id
        
        # Fetch all records
        records = cursor.fetchall()
        return_object['records'] = records
        
        log.info(
            log_keyword,
            "Query executed successfully.",
            query_id=query_id,
            record_count=len(records)
        )
        
        # Empty records is still success
        if len(records) == 0:
            log.info(
                log_keyword,
                "No records found matching criteria (this is not an error).",
                pipeline_name=pipeline_name,
                config_name=config_name,
                config_version=config_version
            )
        else:
            log.info(
                log_keyword,
                "Records fetched successfully.",
                record_count=len(records)
            )
        
        # Set success flag
        return_object['continue_dag_run'] = True
        
        return return_object
        
    except Exception as e:
        error_details = traceback.format_exc()
        error_msg_for_email = (
            f"Fetch Pipeline Config Details Failed!\n\n"
            f"Error Location: fetch_pipeline_config_details\n"
            f"Error Type: {type(e).__name__}\n"
            f"Database: {database_name if database_name else 'N/A'}\n"
            f"Schema: {schema_name if schema_name else 'N/A'}\n"
            f"Table: {table_name if table_name else 'N/A'}\n"
            f"Full Table Name: {three_part_name}\n"
            f"Pipeline Name: {pipeline_name if pipeline_name else 'N/A'}\n"
            f"Config Name: {config_name if config_name else 'N/A'}\n"
            f"Config Version: {config_version if config_version else 'N/A'}\n"
            f"Query ID: {query_id if query_id else 'N/A'}\n\n"
            f"Error Message: {str(e)}\n\n"
            f"Full Traceback:\n{error_details}"
        )
        
        log.error(
            log_keyword,
            "An error occurred during fetch operation.",
            exc_info=True,
            database=database_name if database_name else 'N/A',
            schema=schema_name if schema_name else 'N/A',
            table=table_name if table_name else 'N/A',
            pipeline_name=pipeline_name if pipeline_name else 'N/A',
            config_name=config_name if config_name else 'N/A',
            config_version=config_version if config_version else 'N/A',
            error_message=str(e),
            query_id=query_id if query_id else 'N/A'
        )
        
        return_object['error_message'] = error_msg_for_email
        return_object['continue_dag_run'] = False
        return_object['records'] = []
        return return_object
        
    finally:
        # Clean up resources
        if cursor:
            try:
                cursor.close()
                log.info(
                    log_keyword,
                    "Cursor closed successfully."
                )
            except Exception as e:
                log.error(
                    log_keyword,
                    "Error closing cursor.",
                    error_message=str(e)
                )
        
        if conn:
            try:
                conn.close()
                log.info(
                    log_keyword,
                    "Snowflake connection closed successfully."
                )
            except Exception as e:
                log.error(
                    log_keyword,
                    "Error closing connection.",
                    error_message=str(e)
                )





