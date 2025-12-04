# framework/monitoring_scripts/sf_user_config_request_manager.py

import traceback
from typing import Dict, Any, List, Optional
from snowflake.connector import DictCursor
from utils.custom_logging import CustomChainLogger
from utils.db_utils import safe_close_cursor_and_conn
from utils.sql_formatter_utility import format_and_print_sql_query
from framework.monitoring_scripts.snowflake_connector import get_snowflake_connection


def create_config_table_for_user_requests_if_not_exists(
    config: Dict[str, Any],
    logger: CustomChainLogger,
    query_tag: str = None
) -> Dict[str, Any]:
    """
    Creates the USER_REQUESTED_NEW_CONFIG table if it does not exist.
    
    This table stores user-requested configuration changes for pipelines including:
    - Pipeline and configuration identification
    - Source, stage, and target configurations (as VARIANT/JSON)
    - Request status tracking (PENDING, CONSUMED, FAILED)
    - Retry attempt tracking and timestamps
    
    Args:
        config (Dict[str, Any]): Configuration dictionary containing:
            - 'sf_config_params': Dict with Snowflake connection parameters
            - 'config_table_for_user_requests': Dict with table configuration
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
    log_keyword = "CONFIG_TABLE_FOR_USER_REQUESTS_TABLE_CREATION"
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
        "continue_dag_run": False,
        "error_message": None,
        "query_id": None,
        "table_name": three_part_name,
        "table_exists": False,
    }

    try:
        log.info(
            log_keyword,
            "Starting CONFIG_TABLE_FOR_USER_REQUESTS table creation process.",
            query_tag=query_tag,
        )

        # Extract configuration parameters
        sf_con_parms = config.get("sf_config_params", {}) or {}
        table_config = config.get("config_table_for_user_requests", {}) or {}

        database_name = (
            table_config.get("database_name")
            or sf_con_parms.get("database")
            or "UNKNOWN_DB"
        )
        schema_name = (
            table_config.get("schema_name") or sf_con_parms.get("schema") or "PUBLIC"
        )
        table_name = table_config.get("table_name") or "USER_REQUESTED_NEW_CONFIG"

        # Build three-part name
        three_part_name = f"{database_name}.{schema_name}.{table_name}"
        return_object["table_name"] = three_part_name

        log.info(
            log_keyword,
            "Resolved table configuration.",
            database=database_name,
            schema=schema_name,
            table=table_name,
            full_table_name=three_part_name,
            query_tag=query_tag,
        )

        # Get Snowflake connection
        conn_result = get_snowflake_connection(sf_con_parms, log, QUERY_TAG=query_tag)
        if not conn_result.get("continue_dag_run"):
            msg = conn_result.get("error_message")
            log.error(
                log_keyword, "Failed to get Snowflake connection.", error_message=msg
            )
            return_object["error_message"] = msg
            return return_object

        conn = conn_result.get("conn")
        if conn is None:
            msg = "Snowflake connector returned no connection object"
            log.error(log_keyword, msg)
            return_object["error_message"] = msg
            return return_object

        cursor = conn.cursor()

        # Create table DDL
        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {three_part_name} (
                
                ROW_ID                           NUMBER AUTOINCREMENT START 1 INCREMENT 1 PRIMARY KEY,
                
                PIPELINE_NAME                    VARCHAR(1000) NOT NULL,
                CONFIG_NAME                      VARCHAR(1000) NOT NULL,
                PIPELINE_DESC                    VARCHAR(5000) DEFAULT NULL,
                
                NUMBER_OF_STAGES                 INTEGER DEFAULT 0,
                SOURCE_CONFIG                    VARIANT DEFAULT NULL,
                STAGES_CONFIG                    VARIANT DEFAULT NULL,
                TARGET_CONFIG                    VARIANT DEFAULT NULL,
                IS_ACTIVE                        BOOLEAN DEFAULT TRUE,
                SOURCE_MIN_REQUESTABLE_TIMESTAMP TIMESTAMP_TZ DEFAULT NULL,
                SOURCE_MAX_REQUESTABLE_TIMESTAMP TIMESTAMP_TZ DEFAULT NULL,
                
                OWNER_LIST                       VARIANT DEFAULT NULL,
                PIPELINE_RUN_VALID_FROM_TIMESTAMP TIMESTAMP_TZ DEFAULT NULL,
                PIPELINE_RUN_VALID_TILL_TIMESTAMP TIMESTAMP_TZ DEFAULT NULL,
                UPDATED_BY                       VARCHAR(1000) DEFAULT NULL,
                
                USER_REQUEST_STATUS              VARCHAR(50) DEFAULT 'PENDING',
                RETRY_ATTEMPTS                   INTEGER DEFAULT 0,
                CREATED_AT                       TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
                UPDATED_AT                       TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
                CONSUMED_AT                      TIMESTAMP_TZ DEFAULT NULL
            )
        """

        log.info(
            log_keyword,
            "Executing CREATE TABLE IF NOT EXISTS statement.",
            full_table_name=three_part_name,
        )

        # Print formatted SQL for manual testing/verification
        format_and_print_sql_query(create_table_sql, {})

        cursor.execute(create_table_sql)

        # Capture the query ID
        query_id = cursor.sfqid
        return_object["query_id"] = query_id

        log.info(
            log_keyword,
            "Table created successfully or already exists.",
            full_table_name=three_part_name,
            query_id=query_id,
        )

        # Set success flags
        return_object["table_exists"] = True
        return_object["continue_dag_run"] = True

        return return_object

    except Exception as e:
        error_details = traceback.format_exc()
        error_msg_for_email = (
            f"User Config Request Table Creation Failed!\n\n"
            f"Error Location: create_config_table_for_user_requests_if_not_exists\n"
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
            database=database_name if database_name else "N/A",
            schema=schema_name if schema_name else "N/A",
            table=table_name if table_name else "N/A",
            error_message=str(e),
            query_id=query_id if query_id else "N/A",
        )

        return_object["error_message"] = error_msg_for_email
        return_object["continue_dag_run"] = False
        return return_object

    finally:
        # Clean up resources
        safe_close_cursor_and_conn(cursor, conn, log, log_keyword)


def fetch_oldest_pending_config_request(
    config: Dict[str, Any],
    logger: CustomChainLogger,
    query_tag: str = None
) -> Dict[str, Any]:
    """
    Fetches the oldest PENDING configuration request for a given pipeline and config.
    
    This function retrieves the oldest record from USER_REQUESTED_NEW_CONFIG table
    where USER_REQUEST_STATUS is 'PENDING', ordered by CREATED_AT ascending.
    
    Args:
        config (Dict[str, Any]): Configuration dictionary containing:
            - 'sf_config_params': Dict with Snowflake connection parameters
            - 'config_table_for_user_requests': Dict with table configuration
            - 'fetch_criteria': Dict with PIPELINE_NAME and CONFIG_NAME
        logger (CustomChainLogger): Custom logger instance for tracking function execution.
        query_tag (str, optional): QUERY_TAG for session tracking.
    
    Returns:
        Dict[str, Any]: A dictionary containing:
            - 'continue_dag_run' (bool): True if fetch successful, False otherwise
            - 'error_message' (Optional[str]): Error message if failed, None if successful
            - 'query_id' (Optional[str]): Snowflake query ID
            - 'record' (Optional[Dict]): The fetched record as a dict, or None if no record found
            - 'record_found' (bool): Boolean indicating if a record was found
    """
    log_keyword = "FETCH_OLDEST_PENDING_CONFIG_REQUEST"
    log = logger.new_frame(log_keyword)

    query_id = None
    conn = None
    cursor = None
    database_name = None
    schema_name = None
    table_name = None
    three_part_name = "UNKNOWN.UNKNOWN.UNKNOWN"
    pipeline_name = None
    config_name = None

    return_object = {
        "continue_dag_run": False,
        "error_message": None,
        "query_id": None,
        "record": None,
        "record_found": False,
    }

    try:
        log.info(
            log_keyword,
            "Starting fetch of oldest pending config request.",
            query_tag=query_tag,
        )

        # Extract configuration parameters
        sf_con_parms = config.get("sf_config_params", {}) or {}
        table_config = config.get("config_table_for_user_requests", {}) or {}
        fetch_criteria = config.get("fetch_criteria", {}) or {}

        database_name = (
            table_config.get("database_name")
            or sf_con_parms.get("database")
            or "UNKNOWN_DB"
        )
        schema_name = (
            table_config.get("schema_name") or sf_con_parms.get("schema") or "PUBLIC"
        )
        table_name = (
            table_config.get("table_name") or "USER_REQUESTED_NEW_CONFIG"
        )

        # Build three-part name
        three_part_name = f"{database_name}.{schema_name}.{table_name}"

        # Extract fetch criteria
        pipeline_name = fetch_criteria.get("PIPELINE_NAME")
        config_name = fetch_criteria.get("CONFIG_NAME")

        log.info(
            log_keyword,
            "Configuration extracted.",
            database=database_name,
            schema=schema_name,
            table=table_name,
            full_table_name=three_part_name,
            pipeline_name=pipeline_name,
            config_name=config_name,
            query_tag=query_tag,
        )

        # Get Snowflake connection
        conn_result = get_snowflake_connection(sf_con_parms, log, QUERY_TAG=query_tag)
        if not conn_result.get("continue_dag_run"):
            msg = conn_result.get("error_message")
            log.error(
                log_keyword, "Failed to get Snowflake connection.", error_message=msg
            )
            return_object["error_message"] = msg
            return return_object

        conn = conn_result.get("conn")
        if conn is None:
            msg = "Snowflake connector returned no connection object"
            log.error(log_keyword, msg)
            return_object["error_message"] = msg
            return return_object

        # Create cursor with DictCursor to get results as dictionaries
        cursor = conn.cursor(DictCursor)

        log.info(
            log_keyword,
            "Successfully connected to Snowflake with DictCursor.",
        )

        # Build and execute SELECT query
        select_sql = f"""
            SELECT *
            FROM {three_part_name}
            WHERE PIPELINE_NAME = %(PIPELINE_NAME)s 
              AND CONFIG_NAME = %(CONFIG_NAME)s
              AND USER_REQUEST_STATUS = 'PENDING'
            ORDER BY CREATED_AT ASC
            LIMIT 1
        """

        query_params = {
            "PIPELINE_NAME": pipeline_name,
            "CONFIG_NAME": config_name,
        }

        log.info(
            log_keyword,
            "Executing SELECT query to fetch oldest pending request.",
        )

        # Print formatted SQL for manual testing/verification
        try:
            format_and_print_sql_query(select_sql, query_params)
        except Exception:
            log.debug(log_keyword, "SQL formatter failed; proceeding with raw SQL.")

        cursor.execute(select_sql, query_params)

        # Capture the query ID
        query_id = cursor.sfqid
        return_object["query_id"] = query_id

        # Fetch the record
        record = cursor.fetchone()

        if record is None:
            log.info(
                log_keyword,
                "No pending config request found (this is not an error).",
                pipeline_name=pipeline_name,
                config_name=config_name,
            )
            return_object["continue_dag_run"] = True
            return_object["record_found"] = False
            return return_object

        log.info(
            log_keyword,
            "Fetched oldest pending config request successfully.",
            query_id=query_id,
            row_id=record.get("ROW_ID"),
        )

        # Set success flag and record
        return_object["continue_dag_run"] = True
        return_object["record"] = record
        return_object["record_found"] = True

        return return_object

    except Exception as e:
        error_details = traceback.format_exc()
        error_msg_for_email = (
            f"Fetch Oldest Pending Config Request Failed!\n\n"
            f"Error Location: fetch_oldest_pending_config_request\n"
            f"Error Type: {type(e).__name__}\n"
            f"Database: {database_name if database_name else 'N/A'}\n"
            f"Schema: {schema_name if schema_name else 'N/A'}\n"
            f"Table: {table_name if table_name else 'N/A'}\n"
            f"Full Table Name: {three_part_name}\n"
            f"Pipeline Name: {pipeline_name if pipeline_name else 'N/A'}\n"
            f"Config Name: {config_name if config_name else 'N/A'}\n"
            f"Query ID: {query_id if query_id else 'N/A'}\n\n"
            f"Error Message: {str(e)}\n\n"
            f"Full Traceback:\n{error_details}"
        )

        log.error(
            log_keyword,
            "An error occurred during fetch operation.",
            exc_info=True,
            database=database_name if database_name else "N/A",
            schema=schema_name if schema_name else "N/A",
            table=table_name if table_name else "N/A",
            pipeline_name=pipeline_name if pipeline_name else "N/A",
            config_name=config_name if config_name else "N/A",
            error_message=str(e),
            query_id=query_id if query_id else "N/A",
        )

        return_object["error_message"] = error_msg_for_email
        return_object["continue_dag_run"] = False
        return return_object

    finally:
        # Clean up resources
        safe_close_cursor_and_conn(cursor, conn, log, log_keyword)


def update_config_request_record(
    config: Dict[str, Any],
    logger: CustomChainLogger,
    record: Dict[str, Any],
    query_tag: str = None
) -> Dict[str, Any]:
    """
    Updates a configuration request record in the USER_REQUESTED_NEW_CONFIG table.
    
    This function updates the specified record with new status, timestamps, and retry attempts.
    The UPDATED_AT timestamp is automatically set to CURRENT_TIMESTAMP().
    
    Args:
        config (Dict[str, Any]): Configuration dictionary containing:
            - 'sf_config_params': Dict with Snowflake connection parameters
            - 'config_table_for_user_requests': Dict with table configuration
        logger (CustomChainLogger): Custom logger instance for tracking function execution.
        record (Dict[str, Any]): Record dict containing ROW_ID and fields to update:
            - ROW_ID (required): Row identifier
            - USER_REQUEST_STATUS (optional): New status ('PENDING', 'CONSUMED', 'FAILED')
            - RETRY_ATTEMPTS (optional): Number of retry attempts
            - CONSUMED_AT (optional): ISO 8601 timestamp when consumed
            - UPDATED_BY (optional): User/process updating the record
        query_tag (str, optional): QUERY_TAG for session tracking.
    
    Returns:
        Dict[str, Any]: A dictionary containing:
            - 'continue_dag_run' (bool): True if update successful, False otherwise
            - 'error_message' (Optional[str]): Error message if failed, None if successful
            - 'query_id' (Optional[str]): Snowflake query ID
            - 'rows_updated' (int): Number of rows updated
    """
    log_keyword = "UPDATE_CONFIG_REQUEST_RECORD"
    log = logger.new_frame(log_keyword)

    query_id = None
    conn = None
    cursor = None
    database_name = None
    schema_name = None
    table_name = None
    three_part_name = "UNKNOWN.UNKNOWN.UNKNOWN"

    return_object = {
        "continue_dag_run": False,
        "error_message": None,
        "query_id": None,
        "rows_updated": 0,
    }

    # Allowed columns for update (inline, local to function)
    ALLOWED_UPDATE_COLUMNS = {
        "USER_REQUEST_STATUS",
        "RETRY_ATTEMPTS",
        "CONSUMED_AT",
        "UPDATED_BY",
    }

    try:
        log.info(
            log_keyword,
            "Starting partial update of config request record.",
            query_tag=query_tag,
        )

        # Extract configuration parameters
        sf_con_parms = config.get("sf_config_params", {}) or {}
        table_config = config.get("config_table_for_user_requests", {}) or {}

        database_name = (
            table_config.get("database_name")
            or sf_con_parms.get("database")
            or "UNKNOWN_DB"
        )
        schema_name = (
            table_config.get("schema_name") or sf_con_parms.get("schema") or "PUBLIC"
        )
        table_name = (
            table_config.get("table_name") or "USER_REQUESTED_NEW_CONFIG"
        )

        # Build three-part name
        three_part_name = f"{database_name}.{schema_name}.{table_name}"

        log.info(
            log_keyword,
            "Resolved table configuration.",
            database=database_name,
            schema=schema_name,
            table=table_name,
            full_table_name=three_part_name,
        )

        # Extract ROW_ID from record
        ROW_ID = record.get("ROW_ID")
        if ROW_ID is None:
            msg = "record must contain 'ROW_ID' key"
            log.error(log_keyword, msg)
            return_object["error_message"] = msg
            return return_object

        log.debug(
            log_keyword,
            "Record details",
            row_id=ROW_ID,
            record_keys=list(record.keys()),
        )

        # Get Snowflake connection
        conn_result = get_snowflake_connection(sf_con_parms, log, QUERY_TAG=query_tag)
        if not conn_result.get("continue_dag_run"):
            msg = conn_result.get("error_message")
            log.error(
                log_keyword, "Failed to get Snowflake connection.", error_message=msg
            )
            return_object["error_message"] = msg
            return return_object

        conn = conn_result.get("conn")
        if conn is None:
            msg = "Snowflake connector returned no connection object"
            log.error(log_keyword, msg)
            return_object["error_message"] = msg
            return return_object

        cursor = conn.cursor()

        # Build dynamic SET clause based on allowed columns in record
        set_clauses: List[str] = []
        params: Dict[str, Any] = {}

        for col_name in ALLOWED_UPDATE_COLUMNS:
            if col_name in record and record[col_name] is not None:
                set_clauses.append(f"{col_name} = %({col_name})s")
                params[col_name] = record[col_name]

        # Always update UPDATED_AT
        set_clauses.append("UPDATED_AT = CURRENT_TIMESTAMP()")

        # Add ROW_ID for WHERE clause
        params["ROW_ID"] = ROW_ID

        set_sql = ", ".join(set_clauses)
        update_sql = f"UPDATE {three_part_name} SET {set_sql} WHERE ROW_ID = %(ROW_ID)s"

        log.info(
            log_keyword,
            "Executing UPDATE query.",
            row_id=ROW_ID,
            update_fields=list(set_clauses),
        )

        # Print formatted SQL for manual testing/verification
        try:
            format_and_print_sql_query(update_sql, params)
        except Exception:
            log.debug(log_keyword, "SQL formatter failed; proceeding with raw SQL.")

        # Execute update
        try:
            cursor.execute(update_sql, params)
            query_id = cursor.sfqid
            return_object["query_id"] = query_id
            log.info(
                log_keyword,
                "UPDATE executed successfully.",
                row_id=ROW_ID,
                query_id=query_id,
            )
        except Exception as upd_err:
            tb = traceback.format_exc()
            query_id = getattr(cursor, "sfqid", None)
            msg = f"Failed to execute UPDATE for ROW_ID={ROW_ID}: {str(upd_err)}"
            log.error(
                log_keyword,
                "Update failed.",
                error_message=msg,
                query_id=query_id,
                exc_info=True,
            )
            return_object["error_message"] = f"{msg}\n{tb}"
            return_object["query_id"] = query_id
            return_object["continue_dag_run"] = False
            return return_object

        return_object["continue_dag_run"] = True
        return_object["rows_updated"] = cursor.rowcount

        return return_object

    except Exception as e:
        error_details = traceback.format_exc()
        error_msg_for_email = (
            f"Update Config Request Record Failed!\n\n"
            f"Error Location: update_config_request_record\n"
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
            "An error occurred during update operation.",
            exc_info=True,
            database=database_name if database_name else "N/A",
            schema=schema_name if schema_name else "N/A",
            table=table_name if table_name else "N/A",
            error_message=str(e),
            query_id=query_id if query_id else "N/A",
        )

        return_object["error_message"] = error_msg_for_email
        return_object["continue_dag_run"] = False
        return return_object

    finally:
        # Clean up resources
        safe_close_cursor_and_conn(cursor, conn, log, log_keyword)


def fetch_failed_config_requests_for_retry(
    config: Dict[str, Any],
    logger: CustomChainLogger,
    query_tag: str = None
) -> Dict[str, Any]:
    """
    Fetches FAILED configuration requests eligible for retry.
    
    This function retrieves records from USER_REQUESTED_NEW_CONFIG table where
    USER_REQUEST_STATUS is 'FAILED' and RETRY_ATTEMPTS < max_retry_attempts,
    ordered by CREATED_AT ascending (oldest first).
    
    Args:
        config (Dict[str, Any]): Configuration dictionary containing:
            - 'sf_config_params': Dict with Snowflake connection parameters
            - 'config_table_for_user_requests': Dict with table configuration
            - 'fetch_criteria': Dict with PIPELINE_NAME, CONFIG_NAME, and MAX_RETRY_ATTEMPTS
        logger (CustomChainLogger): Custom logger instance for tracking function execution.
        query_tag (str, optional): QUERY_TAG for session tracking.
    
    Returns:
        Dict[str, Any]: A dictionary containing:
            - 'continue_dag_run' (bool): True if fetch successful, False otherwise
            - 'error_message' (Optional[str]): Error message if failed, None if successful
            - 'query_id' (Optional[str]): Snowflake query ID
            - 'records' (List[Dict]): List of eligible failed records, empty list if none found
            - 'record_count' (int): Number of records found
    """
    log_keyword = "FETCH_FAILED_CONFIG_REQUESTS_FOR_RETRY"
    log = logger.new_frame(log_keyword)

    query_id = None
    conn = None
    cursor = None
    database_name = None
    schema_name = None
    table_name = None
    three_part_name = "UNKNOWN.UNKNOWN.UNKNOWN"
    pipeline_name = None
    config_name = None
    max_retry_attempts = None
    records = []

    return_object = {
        "continue_dag_run": False,
        "error_message": None,
        "query_id": None,
        "records": [],
        "record_count": 0,
    }

    try:
        log.info(
            log_keyword,
            "Starting fetch of failed config requests eligible for retry.",
            query_tag=query_tag,
        )

        # Extract configuration parameters
        sf_con_parms = config.get("sf_config_params", {}) or {}
        table_config = config.get("config_table_for_user_requests", {}) or {}
        fetch_criteria = config.get("fetch_criteria", {}) or {}

        database_name = (
            table_config.get("database_name")
            or sf_con_parms.get("database")
            or "UNKNOWN_DB"
        )
        schema_name = (
            table_config.get("schema_name") or sf_con_parms.get("schema") or "PUBLIC"
        )
        table_name = (
            table_config.get("table_name") or "USER_REQUESTED_NEW_CONFIG"
        )

        # Build three-part name
        three_part_name = f"{database_name}.{schema_name}.{table_name}"

        # Extract fetch criteria
        pipeline_name = fetch_criteria.get("PIPELINE_NAME")
        config_name = fetch_criteria.get("CONFIG_NAME")
        max_retry_attempts = fetch_criteria.get("MAX_RETRY_ATTEMPTS", 3)

        log.info(
            log_keyword,
            "Configuration extracted.",
            database=database_name,
            schema=schema_name,
            table=table_name,
            full_table_name=three_part_name,
            pipeline_name=pipeline_name,
            config_name=config_name,
            max_retry_attempts=max_retry_attempts,
            query_tag=query_tag,
        )

        # Get Snowflake connection
        conn_result = get_snowflake_connection(sf_con_parms, log, QUERY_TAG=query_tag)
        if not conn_result.get("continue_dag_run"):
            msg = conn_result.get("error_message")
            log.error(
                log_keyword, "Failed to get Snowflake connection.", error_message=msg
            )
            return_object["error_message"] = msg
            return return_object

        conn = conn_result.get("conn")
        if conn is None:
            msg = "Snowflake connector returned no connection object"
            log.error(log_keyword, msg)
            return_object["error_message"] = msg
            return return_object

        # Create cursor with DictCursor to get results as dictionaries
        cursor = conn.cursor(DictCursor)

        log.info(
            log_keyword,
            "Successfully connected to Snowflake with DictCursor.",
        )

        # Build and execute SELECT query
        select_sql = f"""
            SELECT *
            FROM {three_part_name}
            WHERE PIPELINE_NAME = %(PIPELINE_NAME)s 
              AND CONFIG_NAME = %(CONFIG_NAME)s
              AND USER_REQUEST_STATUS = 'FAILED'
              AND RETRY_ATTEMPTS < %(MAX_RETRY_ATTEMPTS)s
            ORDER BY CREATED_AT ASC
        """

        query_params = {
            "PIPELINE_NAME": pipeline_name,
            "CONFIG_NAME": config_name,
            "MAX_RETRY_ATTEMPTS": max_retry_attempts,
        }

        log.info(
            log_keyword,
            "Executing SELECT query to fetch failed records eligible for retry.",
        )

        # Print formatted SQL for manual testing/verification
        try:
            format_and_print_sql_query(select_sql, query_params)
        except Exception:
            log.debug(log_keyword, "SQL formatter failed; proceeding with raw SQL.")

        cursor.execute(select_sql, query_params)

        # Capture the query ID
        query_id = cursor.sfqid
        return_object["query_id"] = query_id

        # Fetch all records
        records = cursor.fetchall() or []
        return_object["records"] = records

        if len(records) == 0:
            log.info(
                log_keyword,
                "No failed config requests found eligible for retry (this is not an error).",
                pipeline_name=pipeline_name,
                config_name=config_name,
                max_retry_attempts=max_retry_attempts,
            )
        else:
            log.info(
                log_keyword,
                "Fetched failed config requests eligible for retry successfully.",
                query_id=query_id,
                record_count=len(records),
            )

        # Set success flag
        return_object["continue_dag_run"] = True
        return_object["record_count"] = len(records)

        return return_object

    except Exception as e:
        error_details = traceback.format_exc()
        error_msg_for_email = (
            f"Fetch Failed Config Requests For Retry Failed!\n\n"
            f"Error Location: fetch_failed_config_requests_for_retry\n"
            f"Error Type: {type(e).__name__}\n"
            f"Database: {database_name if database_name else 'N/A'}\n"
            f"Schema: {schema_name if schema_name else 'N/A'}\n"
            f"Table: {table_name if table_name else 'N/A'}\n"
            f"Full Table Name: {three_part_name}\n"
            f"Pipeline Name: {pipeline_name if pipeline_name else 'N/A'}\n"
            f"Config Name: {config_name if config_name else 'N/A'}\n"
            f"Max Retry Attempts: {max_retry_attempts if max_retry_attempts else 'N/A'}\n"
            f"Query ID: {query_id if query_id else 'N/A'}\n\n"
            f"Error Message: {str(e)}\n\n"
            f"Full Traceback:\n{error_details}"
        )

        log.error(
            log_keyword,
            "An error occurred during fetch operation.",
            exc_info=True,
            database=database_name if database_name else "N/A",
            schema=schema_name if schema_name else "N/A",
            table=table_name if table_name else "N/A",
            pipeline_name=pipeline_name if pipeline_name else "N/A",
            config_name=config_name if config_name else "N/A",
            max_retry_attempts=max_retry_attempts if max_retry_attempts else "N/A",
            error_message=str(e),
            query_id=query_id if query_id else "N/A",
        )

        return_object["error_message"] = error_msg_for_email
        return_object["continue_dag_run"] = False
        return_object["records"] = []
        return_object["record_count"] = 0
        return return_object

    finally:
        # Clean up resources
        safe_close_cursor_and_conn(cursor, conn, log, log_keyword)




