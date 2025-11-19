# # my_main_project_folder/framework/generic_table_creator.py
"""
Utility: generic_table_creator
Location: my_main_project_folder/framework/generic_table_creator.py
Purpose: Generic utility for creating any table with consistent error handling and logging
"""

import traceback
from typing import Dict, Any, List
from utils.custom_logging import CustomChainLogger
from utils.sql_formatter_utility import format_and_print_sql_query
from framework.snowflake_connector import get_snowflake_connection


def create_table_generic(
    sf_con_parms: Dict[str, Any],
    table_details: Dict[str, Any],
    log_keyword: str,
    new_frame_name: str,
    create_table_sql: str,
    logger: CustomChainLogger,
    config: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Generic utility function to create any Snowflake table with consistent error handling and logging.
    
    This function abstracts the common logic for table creation, eliminating code duplication
    across different table creation scripts.
    
    Args:
        sf_con_parms (Dict[str, Any]): Snowflake connection parameters containing:
                                       - account, username, password
        table_details (Dict[str, Any]): Table-specific configuration containing:
                                       - {prefix}_table_name
                                       - {prefix}_database_name
                                       - {prefix}_schema_name
                                       - {prefix}_warehouse_name
                                       - {prefix}_role_name
        log_keyword (str): Keyword for logging (e.g., "CREATE_PIPELINE_EXECUTION_TRACKING_TABLE")
        new_frame_name (str): Frame name for logger (e.g., "create_pipeline_execution_tracking_table")
        create_table_sql (str): SQL template with %(table_name)s placeholder
        logger (CustomChainLogger): Custom logger instance
        config (Dict[str, Any]): Full config dictionary (for safety/context)
    
    Returns:
        Dict[str, Any]: A dictionary containing:
            - 'success' (bool): True if table creation/verification succeeded
            - 'message' (str): Descriptive message about the operation
            - 'continue_dag_run' (bool): Whether the DAG should continue execution
            - 'error_message' (str): Error details if operation failed (None if successful)
            - 'cleanup_errors' (List[str]): List of errors that occurred during resource cleanup
                                           Empty list if no cleanup errors occurred
    
    Example:
        >>> result = create_table_generic(
        ...     sf_con_parms=config["sf_con_parms"],
        ...     table_details=config["pipeline_tracking_details"],
        ...     log_keyword="CREATE_PIPELINE_EXECUTION_TRACKING_TABLE",
        ...     new_frame_name="create_pipeline_execution_tracking_table",
        ...     create_table_sql=PIPELINE_EXECUTION_TRACKING_SQL,
        ...     logger=logger,
        ...     config=config
        ... )
    """
    
    log = logger.new_frame(new_frame_name)
    
    return_object: Dict[str, Any] = {
        'success': False,
        'message': None,
        'continue_dag_run': False,
        'error_message': None,
        'cleanup_errors': []
    }
    
    conn = None
    cursor = None
    
    try:
        # Collect all validation errors
        validation_errors: List[str] = []
        
        if not sf_con_parms:
            validation_errors.append("Missing 'sf_con_parms'")
        
        if not table_details:
            validation_errors.append("Missing 'table_details'")
        
        if not log_keyword:
            validation_errors.append("Missing 'log_keyword'")
        
        if not new_frame_name:
            validation_errors.append("Missing 'new_frame_name'")
        
        if not create_table_sql:
            validation_errors.append("Missing 'create_table_sql'")
        
        if not logger:
            validation_errors.append("Missing 'logger'")
        
        if not config:
            validation_errors.append("Missing 'config'")
        
        # Extract table details with specific names
        table_name = table_details.get("table_name") if table_details else None
        database_name = table_details.get("database_name") if table_details else None
        schema_name = table_details.get("schema_name") if table_details else None
        warehouse_name = table_details.get("warehouse_name") if table_details else None
        role_name = table_details.get("role_name") if table_details else None
        
        if not table_name:
            validation_errors.append("Missing 'table_name' in table_details")
        
        if not database_name:
            validation_errors.append("Missing 'database_name' in table_details")
        
        if not schema_name:
            validation_errors.append("Missing 'schema_name' in table_details")
        
        if not warehouse_name:
            validation_errors.append("Missing 'warehouse_name' in table_details")
        
        if not role_name:
            validation_errors.append("Missing 'role_name' in table_details")
        
        # If any validation errors, collect them and exit
        if validation_errors:
            error_details = "\n".join(validation_errors)
            error_msg_for_email = (
                f"Table Creation Failed - Configuration Validation Error!\n\n"
                f"Error Location: create_table_generic\n"
                f"Log Keyword: {log_keyword}\n"
                f"Error Type: ConfigurationError\n\n"
                f"Validation Errors:\n{error_details}"
            )
            
            log.error(
                log_keyword,
                "Configuration validation failed.",
                validation_errors=error_details
            )
            
            return_object['error_message'] = error_msg_for_email
            return return_object
        
        log.info(
            log_keyword,
            "Attempting to establish Snowflake connection for table creation...",
            account=sf_con_parms.get("account"),
            username=sf_con_parms.get("username"),
            database=database_name,
            schema=schema_name,
            warehouse=warehouse_name,
            role=role_name,
            table_name=table_name
        )
        
        # Get Snowflake connection
        conn_result = get_snowflake_connection(sf_con_parms, log)
        
        if not conn_result['continue_dag_run']:
            log.error(
                log_keyword,
                "Failed to establish Snowflake connection.",
                error_message=conn_result['error_message']
            )
            return_object['error_message'] = conn_result['error_message']
            return return_object
        
        conn = conn_result['conn']
        cursor = conn_result['cursor']
        
        log.info(
            log_keyword,
            "Snowflake connection established successfully."
        )
        
        # Set the role, warehouse, database, and schema
        log.info(
            log_keyword,
            "Setting Snowflake context (role, warehouse, database, schema)..."
        )
        
        cursor.execute(f"USE ROLE {role_name}")
        log.info(log_keyword, f"Set role to: {role_name}", role=role_name)
        
        cursor.execute(f"USE WAREHOUSE {warehouse_name}")
        log.info(log_keyword, f"Set warehouse to: {warehouse_name}", warehouse=warehouse_name)
        
        cursor.execute(f"USE DATABASE {database_name}")
        log.info(log_keyword, f"Set database to: {database_name}", database=database_name)
        
        cursor.execute(f"USE SCHEMA {schema_name}")
        log.info(log_keyword, f"Set schema to: {schema_name}", schema=schema_name)
        
        # Prepare query parameters
        query_params: Dict[str, Any] = {
            'table_name': table_name
        }
        
        log.info(
            log_keyword,
            f"Executing CREATE TABLE IF NOT EXISTS statement for {table_name}..."
        )
        
        # Format and print the query for manual testing
        format_and_print_sql_query(create_table_sql, query_params)
        
        # Execute the CREATE TABLE IF NOT EXISTS statement
        cursor.execute(create_table_sql, query_params)
        
        # Get query ID from cursor
        query_id = cursor.sfqid
        
        log.info(
            log_keyword,
            f"{table_name} table is ready (created or already exists).",
            table_name=table_name,
            query_id=query_id,
            rows_affected=cursor.rowcount
        )
        
        return_object['success'] = True
        return_object['message'] = f"{table_name} table created or already exists."
        return_object['continue_dag_run'] = True
        
        return return_object
    
    except Exception as e:
        # Handle any errors during table creation
        
        error_details = traceback.format_exc()
        error_msg_for_email = (
            f"Table Creation Failed!\n\n"
            f"Error Location: create_table_generic\n"
            f"Log Keyword: {log_keyword}\n"
            f"Error Type: {type(e).__name__}\n"
            f"Message: {str(e)}\n\n"
            f"Full Traceback:\n{error_details}"
        )
        
        log.error(
            log_keyword,
            "Failed to create table.",
            exc_info=True,
            error_message=str(e)
        )
        
        return_object['error_message'] = error_msg_for_email
        
        return return_object
    
    finally:
        # Ensure connections are closed
        if cursor is not None:
            try:
                cursor.close()
            except Exception as e:
                cleanup_error = f"Failed to close cursor: {str(e)}"
                return_object['cleanup_errors'].append(cleanup_error)
                log.warning(
                    log_keyword,
                    cleanup_error,
                    error_message=str(e)
                )
        
        if conn is not None:
            try:
                conn.close()
            except Exception as e:
                cleanup_error = f"Failed to close connection: {str(e)}"
                return_object['cleanup_errors'].append(cleanup_error)
                log.warning(
                    log_keyword,
                    cleanup_error,
                    error_message=str(e)
                )



