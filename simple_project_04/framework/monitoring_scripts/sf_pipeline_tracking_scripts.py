# my_main_project_folder/framework/monitoring_scripts/sf_pipeline_tracking_scripts.py

import traceback
import json
from typing import Dict, Any, List, Optional
from snowflake.connector import DictCursor
from utils.custom_logging import CustomChainLogger, setup_logger
from utils.db_utils import safe_close_cursor_and_conn
from utils.sql_formatter_utility import format_and_print_sql_query
from framework.monitoring_scripts.snowflake_connector import get_snowflake_connection

from framework.time_context_script import (
    fill_gap_and_insert_pending_records_for_tracking_table

)


def create_pipeline_run_tracking_table_if_not_exists(
    config: Dict[str, Any],
    logger: CustomChainLogger,
    query_tag: str = None
) -> Dict[str, Any]:
    """
    Create the PIPELINE_RUN_TRACKING table if it does not exist.
    Returns dict with:
        - continue_dag_run   (bool)
        - error_message      (Optional[str])
        - query_id           (Optional[str])
        - table_name         (str)
        - table_exists       (bool)
    """
    log_keyword = "PIPELINE_RUN_TRACKING_TABLE_CREATION"    
    log = logger.new_frame(log_keyword)

    return_object = {
        "continue_dag_run": False,
        "error_message": None,
        "query_id": None,
        "table_name": "UNKNOWN.UNKNOWN.PIPELINE_RUN_TRACKING",
        "table_exists": False,
    }

    conn = None
    cursor = None

    try:
        log.info(log_keyword, "Starting table creation flow.", query_tag=query_tag)

        sf_con_parms = config.get("sf_config_params", {}) or {}
        table_config = config.get("pipeline_run_tracking_details", {}) or {}

        database = table_config.get("database_name") or sf_con_parms.get("database") or "UNKNOWN_DB"
        schema = table_config.get("schema_name") or sf_con_parms.get("schema") or "PUBLIC"
        table = table_config.get("table_name") or "PIPELINE_RUN_TRACKING"

        three_part_name = f"{database}.{schema}.{table}"
        return_object["table_name"] = three_part_name

        log.info(
            log_keyword,
            "Resolved table name.",
            database=database,
            schema=schema,
            table=table,
            full_table_name=three_part_name,
            query_tag=query_tag,
        )

        # ---------------------------------------------------------------------
        #  Get Snowflake connection
        # ---------------------------------------------------------------------
        conn_result = get_snowflake_connection(sf_con_parms, log, QUERY_TAG=query_tag)
        if not conn_result.get("continue_dag_run"):
            msg = conn_result.get("error_message")
            log.error(log_keyword, "Failed to get Snowflake connection.", error_message=msg)
            return_object["error_message"] = msg
            return return_object

        conn = conn_result.get("conn")
        if conn is None:
            msg = "Snowflake connector returned no connection object"
            log.error(log_keyword, msg)
            return_object["error_message"] = msg
            return return_object

        cursor = conn.cursor()
        # PIPELINE_NAME, CONFIG_NAME
        # ---------------------------------------------------------------------
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {three_part_name} (
            TRACKING_ID                     VARCHAR DEFAULT NULL,
            PIPELINE_NAME                   VARCHAR  DEFAULT 'unknown_pipeline_name',
            CONFIG_NAME                     VARCHAR  DEFAULT 'unknown_configuration_name',

            QUERY_WINDOW_START_TIMESTAMP    TIMESTAMP_TZ DEFAULT NULL,
            QUERY_WINDOW_END_TIMESTAMP      TIMESTAMP_TZ DEFAULT NULL,

            QUERY_WINDOW_DAY                DATE    DEFAULT NULL,
            QUERY_WINDOW_DURATION           VARCHAR DEFAULT 'XX',

            PIPELINE_EXECUTION_DETAILS      VARIANT DEFAULT NULL,

            SOURCE_COUNT                    INT     DEFAULT NULL,
            STAGES_COUNT                    VARIANT DEFAULT NULL,
            TARGET_COUNT                    INT     DEFAULT NULL,

            PIPELINE_STATUS                 VARCHAR DEFAULT 'PENDING',

            CREATED_AT                      TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
            UPDATED_AT                      TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
        )
        """

        log.info(log_keyword, "Executing CREATE TABLE IF NOT EXISTS.")

        try:
            format_and_print_sql_query(create_table_sql, {})
        except Exception:
            log.warning(
                log_keyword,
                "SQL formatter failed; executing raw SQL."
            )

        # ---------------------------------------------------------------------
        #  Execute CREATE TABLE
        # ---------------------------------------------------------------------
        try:
            cursor.execute(create_table_sql)
            qid = cursor.sfqid  # safe direct access
            return_object["query_id"] = qid

            log.info(
                log_keyword,
                "Table created or already exists.",
                full_table_name=three_part_name,
                query_id=qid,
            )

            return_object["table_exists"] = True
            return_object["continue_dag_run"] = True
            return return_object

        except Exception as exec_err:
            tb = traceback.format_exc()
            qid = getattr(cursor, "sfqid", None)
            msg = f"Failed to execute CREATE TABLE: {str(exec_err)}"

            log.error(
                log_keyword,
                "CREATE TABLE execution failed.",
                error_message=msg,
                query_id=qid,
            )

            return_object["error_message"] = f"{msg}\n{tb}"
            return_object["query_id"] = qid
            return_object["continue_dag_run"] = False
            return return_object

    except Exception as e:
        tb = traceback.format_exc()
        qid = getattr(cursor, "sfqid", None) if cursor else None
        msg = f"Exception during table creation: {str(e)}"

        log.error(
            log_keyword,
            "Unexpected exception.",
            error_message=msg,
            query_id=qid,
        )

        return_object["error_message"] = f"{msg}\n{tb}"
        return_object["continue_dag_run"] = False
        return return_object

    finally:
        safe_close_cursor_and_conn(cursor, conn, log, log_keyword)

def fetch_pipeline_run_by_tracking_id(
    tracking_id: str,
    config: Dict[str, Any],
    logger: CustomChainLogger,
    query_tag: str = None
) -> Dict[str, Any]:
    """
    Fetch pipeline run tracking rows by TRACKING_ID
    Returns:
      - continue_dag_run (bool)
      - error_message (Optional[str])
      - query_id (Optional[str])
      - records (List[Dict])
    """
    log_keyword = "PIPELINE_RUN_TRACKING_FETCH"    
    log = logger.new_frame(log_keyword)

    return_object = {
        "continue_dag_run": False,
        "error_message": None,
        "query_id": None,
        "records": []
    }

    conn = None
    cursor = None

    try:
        log.info(log_keyword, "Starting fetch by TRACKING_ID.", tracking_id=tracking_id)

        sf_con_parms = config.get("sf_config_params", {}) or {}
        table_config = config.get("pipeline_run_tracking_details", {}) or {}
        database = table_config.get("database_name") or sf_con_parms.get("database") or "UNKNOWN_DB"
        schema = table_config.get("schema_name") or sf_con_parms.get("schema") or "PUBLIC"
        table = table_config.get("table_name") or "PIPELINE_RUN_TRACKING"
        three_part_name = f"{database}.{schema}.{table}"

        # Connect
        conn_result = get_snowflake_connection(sf_con_parms, log, QUERY_TAG=query_tag)
        if not conn_result.get("continue_dag_run"):
            log.error(
                log_keyword,
                "Failed to get Snowflake connection.",
                error_message=conn_result.get("error_message")
            )
            return_object["error_message"] = conn_result.get("error_message")
            return return_object

        conn = conn_result.get("conn")
        if conn is None:
            msg = "Snowflake connector returned no connection object"
            log.error(log_keyword, msg)
            return_object["error_message"] = msg
            return return_object

        cursor = conn.cursor(DictCursor)

        select_sql = f"""
        SELECT *
        FROM {three_part_name}
        WHERE TRACKING_ID = %(tracking_id)s
        """

        params = {"tracking_id": tracking_id}

        # optional formatting — do not fail if formatter raises
        try:
            format_and_print_sql_query(select_sql, params)
        except Exception:
            log.debug(log_keyword, "SQL formatter failed; proceeding with raw SQL.")

        # execute and capture query id on success
        try:
            cursor.execute(select_sql, params)
            qid = cursor.sfqid  # safe direct access after successful execute
            return_object["query_id"] = qid
        except Exception as exec_err:
            tb = traceback.format_exc()
            qid = getattr(cursor, "sfqid", None)
            msg = f"SQL execution failed for TRACKING_ID={tracking_id}: {str(exec_err)}"
            log.error(
                log_keyword,
                "Failed to execute SELECT.",
                error_message=msg,
                query_id=qid
            )
            return_object["error_message"] = f"{msg}\n{tb}"
            return_object["query_id"] = qid
            return return_object

        # fetch rows
        try:
            rows = cursor.fetchall() or []
        except Exception as fetch_err:
            tb = traceback.format_exc()
            msg = f"Failed to fetch rows after query execution: {str(fetch_err)}"
            qid = getattr(cursor, "sfqid", None)
            log.error(log_keyword, "Failed to fetch rows.", error_message=msg, query_id=qid)
            return_object["error_message"] = f"{msg}\n{tb}"
            return_object["query_id"] = qid
            return return_object

        return_object["records"] = rows
        log.info(log_keyword, "Fetch executed.", query_id=return_object["query_id"], record_count=len(rows))

        return_object["continue_dag_run"] = True
        return return_object

    except Exception as e:
        tb = traceback.format_exc()
        qid = getattr(cursor, "sfqid", None) if cursor is not None else None
        err_msg = f"Fetch failed: {str(e)}"
        log.error(log_keyword, "Exception during fetch.", exc_info=True, error_message=str(e), query_id=qid)
        return_object["error_message"] = f"{err_msg}\n{tb}"
        return_object["query_id"] = qid
        return return_object

    finally:
        # use unified cleanup helper
        safe_close_cursor_and_conn(cursor, conn, log, log_keyword)

def fetch_pending_records(
    config: Dict[str, Any],
    logger: CustomChainLogger,
    query_tag: Optional[str] = None
) -> Dict[str, Any]:
    """
    Fetch first N pending pipeline run records from the tracking table.

    Expectations (uppercase config keys):
      - MAX_PENDING_RECORDS : int (limit)
      - PIPELINE_NAME        : Optional[str]
      - CONFIG_NAME          : Optional[str]
      - sf_config_params     : dict
      - pipeline_run_tracking_details : dict with database_name, schema_name, table_name

    Behavior:
      - Filters by PIPELINE_STATUS = 'PENDING' and optional PIPELINE_NAME/CONFIG_NAME/CONFIG_VERSION
      - Orders by QUERY_WINDOW_START_TIMESTAMP ASC
      - Limits to MAX_PENDING_RECORDS
      - NEVER raises; always returns a result dict:
          {
            "continue_dag_run": bool,
            "error_message": Optional[str],
            "query_id": Optional[str],
            "records": List[Dict]
          }

    Logging rules:
      - All logs use the exact keyword log_keyword
      - Info: concise operational summary (no preview)
      - Warning: recoverable / noteworthy conditions
      - Error: fatal to this function
      - Debug: as much useful internal state as possible (including full_rows)
    """
    log_keyword = "FETCH_PENDING_RECORDS"    
    log = logger.new_frame(log_keyword)

    result: Dict[str, Any] = {
        "continue_dag_run": False,
        "error_message": None,
        "query_id": None,
        "records": []
    }

    conn = None
    cursor = None

    try:
        # Info: starting
        log.info(log_keyword, "Starting fetch of pending pipeline runs.", query_tag=query_tag)

        # Minimal config reads (uppercase-only keys)
        if "MAX_PENDING_RECORDS" not in config:
            msg = "config must include 'MAX_PENDING_RECORDS' (uppercase key required)"
            log.error(log_keyword, msg)
            result["error_message"] = msg
            return result

        try:
            MAX_PENDING_RECORDS = int(config["MAX_PENDING_RECORDS"])
        except Exception:
            msg = f"MAX_PENDING_RECORDS must be an integer; got: {config.get('MAX_PENDING_RECORDS')!r}"
            log.error(log_keyword, msg)
            result["error_message"] = msg
            return result

        # Read optional uppercase filters only (no lowercase fallbacks)
        PIPELINE_NAME = config.get("PIPELINE_NAME")
        CONFIG_NAME = config.get("CONFIG_NAME")

        # Debug: show resolved inputs (safe; do not include secrets)
        log.debug(
            log_keyword,
            "Resolved inputs",
            inputs={
                "MAX_PENDING_RECORDS": MAX_PENDING_RECORDS,
                "PIPELINE_NAME": PIPELINE_NAME,
                "CONFIG_NAME": CONFIG_NAME,
                "query_tag": query_tag
            }
        )

        # Resolve Snowflake connection params and target table (nested keys may be lowercase in that dict)
        sf_con_parms = config.get("sf_config_params", {}) or {}
        table_config = config.get("pipeline_run_tracking_details", {}) or {}
        database = table_config.get("database_name") or sf_con_parms.get("database") or "UNKNOWN_DB"
        schema = table_config.get("schema_name") or sf_con_parms.get("schema") or "PUBLIC"
        table = table_config.get("table_name") or "PIPELINE_RUN_TRACKING"
        three_part_name = f"{database}.{schema}.{table}"

        log.info(
            log_keyword,
            "Resolved Snowflake target and initial settings.",
            target_table=three_part_name,
            requested_limit=MAX_PENDING_RECORDS
        )

        # Obtain connection via helper
        conn_result = get_snowflake_connection(sf_con_parms, log, QUERY_TAG=query_tag)
        if not conn_result.get("continue_dag_run"):
            msg = conn_result.get("error_message") or "Failed to obtain Snowflake connection"
            log.error(log_keyword, "Failed to get Snowflake connection.", error_message=msg)
            result["error_message"] = msg
            return result

        conn = conn_result.get("conn")
        if conn is None:
            msg = "Snowflake connector returned no connection object"
            log.error(log_keyword, msg)
            result["error_message"] = msg
            return result

        cursor = conn.cursor(DictCursor)

        # Build WHERE clause (uppercase column names only)
        where_clauses: List[str] = ["PIPELINE_STATUS = %(PIPELINE_STATUS)s"]
        params: Dict[str, Any] = {"PIPELINE_STATUS": "PENDING"}

        if PIPELINE_NAME is not None:
            where_clauses.append("PIPELINE_NAME = %(PIPELINE_NAME)s")
            params["PIPELINE_NAME"] = PIPELINE_NAME
        if CONFIG_NAME is not None:
            where_clauses.append("CONFIG_NAME = %(CONFIG_NAME)s")
            params["CONFIG_NAME"] = CONFIG_NAME

        where_sql = " AND ".join(where_clauses)

        select_sql = f"""
        SELECT *
        FROM {three_part_name}
        WHERE {where_sql}
        ORDER BY QUERY_WINDOW_START_TIMESTAMP ASC
        LIMIT %(LIMIT)s
        """

        params["LIMIT"] = MAX_PENDING_RECORDS

        # Debug: full SQL + params
        try:
            format_and_print_sql_query(select_sql, params)
            log.debug(log_keyword, "Final SQL prepared for execution", sql=select_sql, params=params)
        except Exception:
            log.warning(log_keyword, "format_and_print_sql_query failed; proceeding without formatted print")
            log.debug(log_keyword, "Raw SQL", sql=select_sql, params=params)

        # Execute query
        try:
            cursor.execute(select_sql, params)
            # safe direct access on success
            result["query_id"] = cursor.sfqid
        except Exception as exec_err:
            tb = traceback.format_exc()
            msg = f"SQL execution failed: {str(exec_err)}"
            qid = getattr(cursor, "sfqid", None)
            log.error(log_keyword, "Failed to execute SELECT on tracking table.", error_message=msg, query_id=qid)
            result["error_message"] = f"{msg}\n{tb}"
            result["query_id"] = qid
            return result

        # Capture query id and fetch rows
        try:
            rows = cursor.fetchall() or []
        except Exception as fetch_err:
            tb = traceback.format_exc()
            msg = f"Failed to fetch rows after query execution: {str(fetch_err)}"
            qid = getattr(cursor, "sfqid", None)
            log.error(log_keyword, "Failed to fetch rows.", error_message=msg, query_id=qid)
            result["error_message"] = f"{msg}\n{tb}"
            result["query_id"] = qid
            result["continue_dag_run"] = False
            return result

        result["records"] = rows
        result["continue_dag_run"] = True

        # Info-level: concise summary (no preview)
        log.info(
            log_keyword,
            "Fetched pending pipeline runs.",
            query_id=result["query_id"],
            requested_limit=MAX_PENDING_RECORDS,
            returned_count=len(rows)
        )

        # Debug: full rows (detailed preview only at debug level)
        try:
            log.debug(log_keyword, "Full fetched rows (debug)", full_rows=rows)
        except Exception:
            log.debug(log_keyword, "Could not serialize full rows in debug log.", exc_info=True)

        return result

    except Exception as e:
        tb = traceback.format_exc()
        err_msg = f"Unexpected failure in fetch_pending_records: {str(e)}"
        log.error(log_keyword, "Unhandled exception in fetch_pending_records.", error_message=err_msg)
        result["error_message"] = f"{err_msg}\n{tb}"
        result["continue_dag_run"] = False
        return result

    finally:
        # unified cleanup helper
        safe_close_cursor_and_conn(cursor, conn, log, log_keyword)

def fetch_running_records(
    config: Dict[str, Any],
    logger: CustomChainLogger,
    query_tag: Optional[str] = None
) -> Dict[str, Any]:
    """
    Fetch all records with PIPELINE_STATUS = 'RUNNING', applying optional filters:
      - PIPELINE_NAME (uppercase key)
      - CONFIG_NAME   (uppercase key)

    Returns (always, never raises):
      {
        "continue_dag_run": bool,
        "error_message": Optional[str],
        "query_id": Optional[str],
        "records": List[Dict]
      }

    Logging keyword (immutable): log_keyword
    """
    log_keyword = "FETCH_RUNNING_RECORDS"    
    log = logger.new_frame(log_keyword)

    result: Dict[str, Any] = {
        "continue_dag_run": False,
        "error_message": None,
        "query_id": None,
        "records": []
    }

    conn = None
    cursor = None

    try:
        log.info(log_keyword, "Starting fetch of RUNNING pipeline records.", query_tag=query_tag)

        # Read uppercase-only filters
        PIPELINE_NAME = config.get("PIPELINE_NAME")
        CONFIG_NAME = config.get("CONFIG_NAME")


        # Debug: resolved filters
        log.debug(
            log_keyword,
            "Resolved inputs for fetch_running_records",
            inputs={
                "PIPELINE_NAME": PIPELINE_NAME,
                "CONFIG_NAME": CONFIG_NAME,
                "query_tag": query_tag
            }
        )

        # Resolve Snowflake connection params and table
        sf_con_parms = config.get("sf_config_params", {}) or {}
        table_config = config.get("pipeline_run_tracking_details", {}) or {}
        database = table_config.get("database_name") or sf_con_parms.get("database") or "UNKNOWN_DB"
        schema = table_config.get("schema_name") or sf_con_parms.get("schema") or "PUBLIC"
        table = table_config.get("table_name") or "PIPELINE_RUN_TRACKING"
        three_part_name = f"{database}.{schema}.{table}"

        log.info(log_keyword, "Resolved Snowflake target for running fetch.", target_table=three_part_name)

        # Obtain connection
        conn_result = get_snowflake_connection(sf_con_parms, log, QUERY_TAG=query_tag)
        if not conn_result.get("continue_dag_run"):
            msg = conn_result.get("error_message") or "Failed to obtain Snowflake connection"
            log.error(log_keyword, "Failed to get Snowflake connection.", error_message=msg)
            result["error_message"] = msg
            return result

        conn = conn_result.get("conn")
        if conn is None:
            msg = "Snowflake connector returned no connection object"
            log.error(log_keyword, msg)
            result["error_message"] = msg
            return result

        cursor = conn.cursor(DictCursor)

        # Build WHERE clause (uppercase columns)
        where_clauses: List[str] = ["PIPELINE_STATUS = 'RUNNING'"]
        params: Dict[str, Any] = {}

        if PIPELINE_NAME is not None:
            where_clauses.append("PIPELINE_NAME = %(PIPELINE_NAME)s")
            params["PIPELINE_NAME"] = PIPELINE_NAME
        if CONFIG_NAME is not None:
            where_clauses.append("CONFIG_NAME = %(CONFIG_NAME)s")
            params["CONFIG_NAME"] = CONFIG_NAME

        where_sql = " AND ".join(where_clauses)

        select_sql = f"""
        SELECT *
        FROM {three_part_name}
        WHERE {where_sql}
        ORDER BY QUERY_WINDOW_START_TIMESTAMP ASC
        """

        # Debug: show SQL + params (formatter optional)
        try:
            format_and_print_sql_query(select_sql, params)
            log.debug(log_keyword, "Final SQL prepared for execution", sql=select_sql, params=params)
        except Exception:
            log.debug(log_keyword, "format_and_print_sql_query failed; proceeding with raw SQL", sql=select_sql, params=params)

        # Execute query
        try:
            cursor.execute(select_sql, params)
            # safe direct access on success
            result["query_id"] = cursor.sfqid
        except Exception as exec_err:
            tb = traceback.format_exc()
            msg = f"SQL execution failed: {str(exec_err)}"
            qid = getattr(cursor, "sfqid", None)
            log.error(log_keyword, "Failed to execute SELECT on tracking table.", error_message=msg, query_id=qid)
            result["error_message"] = f"{msg}\n{tb}"
            result["query_id"] = qid
            return result

        # Fetch rows
        try:
            rows = cursor.fetchall() or []
        except Exception as fetch_err:
            tb = traceback.format_exc()
            msg = f"Failed to fetch rows after query execution: {str(fetch_err)}"
            qid = getattr(cursor, "sfqid", None)
            log.error(log_keyword, "Failed to fetch rows.", error_message=msg, query_id=qid)
            result["error_message"] = f"{msg}\n{tb}"
            result["query_id"] = qid
            result["continue_dag_run"] = False
            return result

        result["records"] = rows
        result["continue_dag_run"] = True

        # Info: concise summary
        log.info(
            log_keyword,
            "Fetched RUNNING pipeline records.",
            query_id=result["query_id"],
            returned_count=len(rows)
        )

        # Debug: full rows
        try:
            log.debug(log_keyword, "Full fetched RUNNING rows (debug)", full_rows=rows)
        except Exception:
            log.debug(log_keyword, "Could not serialize full rows in debug log.", exc_info=True)

        return result

    except Exception as e:
        tb = traceback.format_exc()
        err_msg = f"Unexpected failure in fetch_running_records: {str(e)}"
        qid = getattr(cursor, "sfqid", None) if cursor is not None else None
        log.error(log_keyword, "Unhandled exception in fetch_running_records.", error_message=err_msg, query_id=qid)
        result["error_message"] = f"{err_msg}\n{tb}"
        result["continue_dag_run"] = False
        result["query_id"] = qid
        return result

    finally:
        safe_close_cursor_and_conn(cursor, conn, log, log_keyword)

def update_tracking_record(
    config: Dict[str, Any],
    logger: CustomChainLogger,
    record: Dict[str, Any],
    query_tag: Optional[str] = None
) -> Dict[str, Any]:
    """
    Update a single tracking row identified by TRACKING_ID.
    """
    log_keyword =  "UPDATE_TRACKING_RECORD"      
    log = logger.new_frame(log_keyword)
 
    result: Dict[str, Any] = {
        "continue_dag_run": False,
        "error_message": None,
        "query_id": None,
        "records": []
    }

    # ---------------------------------------------------------------
    # Allowed columns (INLINE — LOCAL TO FUNCTION)
    # Must match the tracking table EXACTLY.
    # ---------------------------------------------------------------
    _ALLOWED_COLUMNS = [
        "TRACKING_ID",
        "PIPELINE_NAME",
        "CONFIG_NAME",
        "QUERY_WINDOW_START_TIMESTAMP",
        "QUERY_WINDOW_END_TIMESTAMP",
        "QUERY_WINDOW_DAY",
        "QUERY_WINDOW_DURATION",
        "PIPELINE_EXECUTION_DETAILS",   # VARIANT
        "SOURCE_COUNT",
        "STAGES_COUNT",                 # VARIANT
        "TARGET_COUNT",
        "PIPELINE_STATUS",
        "CREATED_AT",
        "UPDATED_AT",
    ]

    # Variant columns (INLINE)
    _VARIANT_COLUMNS = [
        "PIPELINE_EXECUTION_DETAILS",
        "STAGES_COUNT",
    ]

    conn = None
    cursor = None

    try:
        log.info(
            log_keyword,
            "Starting partial update for tracking record.",
            tracking_id=record.get("TRACKING_ID"),
            query_tag=query_tag
        )

        # TRACKING_ID required
        TRACKING_ID = record.get("TRACKING_ID")
        if not TRACKING_ID:
            msg = "record must include TRACKING_ID (uppercase key required)"
            log.error(log_keyword, msg)
            result["error_message"] = msg
            return result

        # Resolve table
        sf_con_parms = config.get("sf_config_params", {}) or {}
        table_config = config.get("pipeline_run_tracking_details", {}) or {}
        database = table_config.get("database_name") or sf_con_parms.get("database") or "UNKNOWN_DB"
        schema = table_config.get("schema_name") or sf_con_parms.get("schema") or "PUBLIC"
        table = table_config.get("table_name") or "PIPELINE_RUN_TRACKING"
        three_part_name = f"{database}.{schema}.{table}"

        # Connect
        conn_result = get_snowflake_connection(sf_con_parms, log, QUERY_TAG=query_tag)
        if not conn_result.get("continue_dag_run"):
            msg = conn_result.get("error_message") or "Failed to obtain Snowflake connection"
            log.error(log_keyword, "Failed to get Snowflake connection.", error_message=msg)
            result["error_message"] = msg
            return result

        conn = conn_result.get("conn")
        if conn is None:
            msg = "Snowflake connector returned no connection object"
            log.error(log_keyword, msg)
            result["error_message"] = msg
            return result

        cursor = conn.cursor()

        # ---------------------------------------------------------------------
        # Build SET clause
        # ---------------------------------------------------------------------
        set_clauses: List[str] = []
        params: Dict[str, Any] = {}

        for col in _ALLOWED_COLUMNS:
            # skip TRACKING_ID (WHERE) and CREATED_AT (never updated)
            if col in ("TRACKING_ID", "CREATED_AT"):
                continue
            if col not in record:
                continue

            val = record[col]
            param_name = col

            if col in _VARIANT_COLUMNS:
                # Handle VARIANT columns
                if val is None:
                    set_clauses.append(f"{col} = NULL")
                else:
                    try:
                        json_text = json.dumps(val)
                    except Exception:
                        json_text = json.dumps(str(val))
                    set_clauses.append(f"{col} = PARSE_JSON(%({param_name})s)")
                    params[param_name] = json_text
            else:
                # scalar column
                set_clauses.append(f"{col} = %({param_name})s")
                params[param_name] = val

        # Always update UPDATED_AT
        set_clauses.append("UPDATED_AT = CURRENT_TIMESTAMP()")

        # Add TRACKING_ID for WHERE clause
        params["TRACKING_ID"] = TRACKING_ID

        set_sql = ", ".join(set_clauses)
        update_sql = f"UPDATE {three_part_name} SET {set_sql} WHERE TRACKING_ID = %(TRACKING_ID)s"

        # Debug SQL
        try:
            format_and_print_sql_query(update_sql, params)
            log.debug(log_keyword, "Prepared UPDATE", sql=update_sql, params=params, full_record=record)
        except Exception:
            log.debug(log_keyword, "Prepared UPDATE (raw)", sql=update_sql, params=params)

        # Execute update
        try:
            cursor.execute(update_sql, params)
            qid = cursor.sfqid
            result["query_id"] = qid
            log.info(log_keyword, "UPDATE executed", tracking_id=TRACKING_ID, query_id=qid)
        except Exception as upd_err:
            tb = traceback.format_exc()
            qid = getattr(cursor, "sfqid", None)
            msg = f"Failed to execute UPDATE for TRACKING_ID={TRACKING_ID}: {str(upd_err)}"
            log.error(log_keyword, "Update failed.", error_message=msg, query_id=qid)
            result["error_message"] = f"{msg}\n{tb}"
            result["query_id"] = qid
            result["continue_dag_run"] = False
            return result

        result["continue_dag_run"] = True
        result["records"] = []
        return result

    except Exception as e:
        tb = traceback.format_exc()
        qid_fallback = None
        try:
            qid_fallback = getattr(cursor, "sfqid", None)
        except Exception:
            pass

        err_msg = f"Unexpected failure in update_tracking_record: {str(e)}"
        log.error(log_keyword, "Unhandled exception in update_tracking_record.",
                  error_message=err_msg, query_id=qid_fallback)
        result["error_message"] = f"{err_msg}\n{tb}"
        result["continue_dag_run"] = False
        result["query_id"] = qid_fallback
        return result

    finally:
        safe_close_cursor_and_conn(cursor, conn, log, log_keyword)

def check_table_exists_from_config(
    config: Dict[str, Any],
    logger: CustomChainLogger,
    query_tag: Optional[str] = None
) -> Dict[str, Any]:
    
    log_keyword =  "CHECK_TABLE_EXISTS"
    log = logger.new_frame(log_keyword)

    return_object: Dict[str, Any] = {
        "table_exists": None,
        "continue_dag_run": False,
        "error_message": None,
    }

    try:
        # Read tracking-table details (NOTE: trailing space is intentional)
        tracking_table_details = config["tracking_table_details "]

        db = tracking_table_details["database"]
        sch = tracking_table_details["schema"]
        tbl = tracking_table_details["table"]

        db_upper = db.strip().upper()
        sch_upper = sch.strip().upper()
        tbl_upper = tbl.strip().upper()

        # Ensure sf_config_params exists for connection
        sf_conn_params = config.get("sf_config_params", {})
        if not sf_conn_params:
            msg = "Missing 'sf_config_params' in config."
            log.error(log_keyword, msg)
            return_object["error_message"] = msg
            return return_object

        # Reuse your connection function
        conn_result = get_snowflake_connection(sf_conn_params, logger, QUERY_TAG=query_tag)
        if not conn_result.get("continue_dag_run"):
            # propagate connection error
            err = conn_result.get("error_message")
            log.error(log_keyword, "Failed to obtain Snowflake connection.", error_message=err)
            return_object["error_message"] = err
            return return_object

        conn = conn_result["conn"]
        cursor = conn_result["cursor"] 

        if cursor is None:
            msg = "Connection returned no cursor."
            log.error(log_keyword, msg)
            return_object["error_message"] = msg
            try:
                if conn:
                    conn.close()
            except Exception:
                log.debug(log_keyword, "Failed to close connection (ignored).", exc_info=True)
            return return_object

        # SQL with pyformat style placeholders for your formatter
        sql_template = f"""
            SELECT 1
            FROM {db_upper}.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = %(TABLE_SCHEMA)s
              AND TABLE_NAME = %(TABLE_NAME)s
            LIMIT 1;
        """

        params_dict = {
            "TABLE_SCHEMA": sch_upper,
            "TABLE_NAME": tbl_upper
        }

        # Safely print formatted SQL
        try:
            format_and_print_sql_query(sql_template, params_dict)
        except Exception:
            log.debug(log_keyword, "format_and_print_sql_query failed (ignored).", exc_info=True)

        log.info(
            log_keyword,
            "Executing INFORMATION_SCHEMA.TABLES lookup.",
            database=db_upper,
            schema=sch_upper,
            table=tbl_upper
        )

        cursor.execute(sql_template, params_dict)
        row = cursor.fetchone()

        exists_flag = row is not None

        log.info(
            log_keyword,
            "Table existence check completed.",
            database=db_upper,
            schema=sch_upper,
            table=tbl_upper,
            table_exists=exists_flag
        )

        return_object["table_exists"] = exists_flag
        return_object["continue_dag_run"] = True

        # Close resources following your framework pattern
        try:
            cursor.close()
        except Exception:
            log.debug(log_keyword, "Failed to close cursor (ignored).", exc_info=True)

        try:
            if conn:
                conn.close()
        except Exception:
            log.debug(log_keyword, "Failed to close connection (ignored).", exc_info=True)

        return return_object

    except Exception as e:
        error_details = traceback.format_exc()

        error_msg = (
            f"Table existence check failed!\n\n"
            f"Database: {config.get('tracking_table_details ', {}).get('database')}\n"
            f"Schema: {config.get('tracking_table_details ', {}).get('schema')}\n"
            f"Table: {config.get('tracking_table_details ', {}).get('table')}\n"
            f"Error Type: {type(e).__name__}\n"
            f"Message: {str(e)}\n\n"
            f"--- Full Traceback ---\n{error_details}"
        )

        log.error(
            log_keyword,
            "Unexpected error during table existence check.",
            exc_info=True,
            error_message=str(e)
        )

        return_object["error_message"] = error_msg
        return_object["continue_dag_run"] = False
        return return_object


def fetch_last_completed_window_end(
    config: Dict[str, Any],
    logger: CustomChainLogger,
    query_tag: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Fetch the MAX(QUERY_WINDOW_END_TIMESTAMP) for a given pipeline_name and config_name
    from the tracking table, restricted to PIPELINE_STATUS = 'COMPLETED'.

    Inputs expected in config (lowercase keys, as per new orchestration layer):
      - pipeline_name : str
      - config_name   : str
      - sf_config_params : dict
      - pipeline_run_tracking_details : dict with:
            - database_name
            - schema_name
            - table_name

    Returns:
      {
        "continue_dag_run": bool,
        "error_message": Optional[str],
        "last_completed_end": Optional[datetime],  # None if no completed rows
      }
    """
    log_keyword = "FETCH_LAST_COMPLETED_WINDOW_END"
    log = logger.new_frame(log_keyword)

    result: Dict[str, Any] = {
        "continue_dag_run": False,
        "error_message": None,
        "last_completed_end": None,
    }

    conn = None
    cursor = None

    try:
        # ------------------------------------------------------------------
        # Read identifiers from config (lowercase keys as per new design)
        # ------------------------------------------------------------------
        pipeline_name = config["pipeline_name"]
        config_name = config["config_name"]

        log.info(
            log_keyword,
            "Starting fetch_last_completed_window_end.",
            pipeline_name=pipeline_name,
            config_name=config_name,
            query_tag=query_tag,
        )

        # Resolve Snowflake connection params and tracking table details
        sf_con_parms = config.get("sf_config_params", {}) or {}
        table_config = config.get("pipeline_run_tracking_details", {}) or {}

        database = table_config.get("database_name") or sf_con_parms.get("database") or "UNKNOWN_DB"
        schema = table_config.get("schema_name") or sf_con_parms.get("schema") or "PUBLIC"
        table = table_config.get("table_name") or "PIPELINE_RUN_TRACKING"

        three_part_name = f"{database}.{schema}.{table}"

        log.info(
            log_keyword,
            "Resolved Snowflake tracking table for last_completed_end fetch.",
            target_table=three_part_name,
        )

        # ------------------------------------------------------------------
        # Obtain Snowflake connection
        # ------------------------------------------------------------------
        conn_result = get_snowflake_connection(sf_con_parms, log, QUERY_TAG=query_tag)
        if not conn_result.get("continue_dag_run"):
            msg = conn_result.get("error_message") or "Failed to obtain Snowflake connection"
            log.error(
                log_keyword,
                "Failed to get Snowflake connection.",
                error_message=msg,
            )
            result["error_message"] = msg
            return result

        conn = conn_result.get("conn")
        if conn is None:
            msg = "Snowflake connector returned no connection object"
            log.error(log_keyword, msg)
            result["error_message"] = msg
            return result

        cursor = conn.cursor(DictCursor)

        # ------------------------------------------------------------------
        # Build SQL for MAX(QUERY_WINDOW_END_TIMESTAMP)
        # ------------------------------------------------------------------
        select_sql = f"""
        SELECT
            MAX(QUERY_WINDOW_END_TIMESTAMP) AS LAST_COMPLETED_END
        FROM {three_part_name}
        WHERE PIPELINE_STATUS = 'COMPLETED'
          AND PIPELINE_NAME = %(PIPELINE_NAME)s
          AND CONFIG_NAME = %(CONFIG_NAME)s
        """

        params: Dict[str, Any] = {
            "PIPELINE_NAME": pipeline_name,
            "CONFIG_NAME": config_name,
        }

        # Optional pretty-print of SQL
        try:
            format_and_print_sql_query(select_sql, params)
            log.debug(
                log_keyword,
                "Final SQL prepared for last_completed_end fetch",
                sql=select_sql,
                params=params,
            )
        except Exception:
            log.debug(
                log_keyword,
                "format_and_print_sql_query failed; proceeding with raw SQL for last_completed_end fetch",
                sql=select_sql,
                params=params,
            )

        # ------------------------------------------------------------------
        # Execute query
        # ------------------------------------------------------------------
        try:
            cursor.execute(select_sql, params)
            # We do not expose query_id here, but we may still log it
            qid = getattr(cursor, "sfqid", None)
        except Exception as exec_err:
            import traceback
            tb = traceback.format_exc()
            qid = getattr(cursor, "sfqid", None) if cursor is not None else None
            msg = f"SQL execution failed while fetching last completed window end: {str(exec_err)}"
            log.error(
                log_keyword,
                "Failed to execute MAX SELECT on tracking table.",
                error_message=msg,
                query_id=qid,
            )
            result["error_message"] = f"{msg}\n{tb}"
            return result

        # ------------------------------------------------------------------
        # Fetch result row
        # ------------------------------------------------------------------
        try:
            row = cursor.fetchone()
        except Exception as fetch_err:
            import traceback
            tb = traceback.format_exc()
            qid = getattr(cursor, "sfqid", None) if cursor is not None else None
            msg = f"Failed to fetch row for last completed window end: {str(fetch_err)}"
            log.error(
                log_keyword,
                "Failed to fetch row after MAX SELECT execution.",
                error_message=msg,
                query_id=qid,
            )
            result["error_message"] = f"{msg}\n{tb}"
            return result

        last_completed_end = None
        if row:
            # DictCursor returns columns by alias
            last_completed_end = row.get("LAST_COMPLETED_END")

        result["last_completed_end"] = last_completed_end
        result["continue_dag_run"] = True

        log.info(
            log_keyword,
            "Fetched last completed window end.",
            pipeline_name=pipeline_name,
            config_name=config_name,
            last_completed_end=str(last_completed_end) if last_completed_end is not None else None,
        )

        return result

    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        err_msg = f"Unexpected failure in fetch_last_completed_window_end: {str(e)}"
        log.error(
            log_keyword,
            "Unhandled exception in fetch_last_completed_window_end.",
            error_message=err_msg,
        )
        result["error_message"] = f"{err_msg}\n{tb}"
        result["continue_dag_run"] = False
        return result

    finally:
        safe_close_cursor_and_conn(cursor, conn, log, log_keyword)


def insert_multiple_records_into_tracking_table(
    records: List[Dict[str, Any]],
    config: Dict[str, Any],
    logger: CustomChainLogger,
    query_tag: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Bulk insert multiple tracking-table-style records into the tracking table.

    This function:
      - Inserts full records into the Snowflake tracking table.
      - Handles VARIANT columns via PARSE_JSON().
      - Uses executemany for bulk insertion.
      - Does NOT raise exceptions; returns {continue_dag_run, error_message}.

    Inputs:
        records: list of dicts shaped like tracking table rows
        config: dict containing:
            - config["sf_config_params"]
            - config["pipeline_run_tracking_details"]
        logger: CustomChainLogger
        query_tag: optional query tag

    Returns:
      {
          "continue_dag_run": bool,
          "error_message": Optional[str],
          "inserted_row_count": int,
          "query_id": Optional[str],
      }
    """

    log_keyword = "INSERT_MULTIPLE_RECORDS_INTO_TRACKING_TABLE"
    log = logger.new_frame(log_keyword)

    result: Dict[str, Any] = {
        "continue_dag_run": True,
        "error_message": None,
        "inserted_row_count": 0,
        "query_id": None,
    }

    conn = None
    cursor = None

    try:
        if not records:
            log.info(
                log_keyword,
                "No records provided for bulk insert. Nothing to do.",
            )
            return result

        log.info(
            log_keyword,
            "Starting bulk insert into tracking table.",
            TOTAL_RECORDS=len(records),
        )

        # -----------------------------------------------------------
        # Resolve table and Snowflake config
        # -----------------------------------------------------------
        sf_con_parms: Dict[str, Any] = config["sf_config_params"]
        tracking_details: Dict[str, Any] = config["pipeline_run_tracking_details"]

        database_name: str = tracking_details["database_name"]
        schema_name: str = tracking_details["schema_name"]
        table_name: str = tracking_details["table_name"]

        fully_qualified_table_name = f"{database_name}.{schema_name}.{table_name}"

        log.info(
            log_keyword,
            "Resolved tracking table.",
            TRACKING_TABLE=fully_qualified_table_name,
        )

        # -----------------------------------------------------------
        # Establish Snowflake connection
        # -----------------------------------------------------------
        conn_result = get_snowflake_connection(sf_con_parms, log, QUERY_TAG=query_tag)
        if not conn_result.get("continue_dag_run", False):
            error_message = (
                f"[{log_keyword}] Failed to obtain Snowflake connection. "
                f"Underlying error: {conn_result.get('error_message')}"
            )
            result["continue_dag_run"] = False
            result["error_message"] = error_message

            log.error(log_keyword, error_message)
            return result

        conn = conn_result.get("conn")
        if conn is None:
            error_message = f"[{log_keyword}] Snowflake connection object is None."
            result["continue_dag_run"] = False
            result["error_message"] = error_message

            log.error(log_keyword, error_message)
            return result

        cursor = conn.cursor(DictCursor)

        # -----------------------------------------------------------
        # Prepare INSERT statement
        # -----------------------------------------------------------
        columns = [
            "TRACKING_ID",
            "PIPELINE_NAME",
            "CONFIG_NAME",
            "QUERY_WINDOW_START_TIMESTAMP",
            "QUERY_WINDOW_END_TIMESTAMP",
            "QUERY_WINDOW_DAY",
            "QUERY_WINDOW_DURATION",
            "PIPELINE_EXECUTION_DETAILS",   # VARIANT
            "SOURCE_COUNT",
            "STAGES_COUNT",                 # VARIANT
            "TARGET_COUNT",
            "PIPELINE_STATUS",
            "CREATED_AT",
            "UPDATED_AT",
        ]

        column_list_sql = ", ".join(columns)

        # Values expression (PARSE_JSON for VARIANT)
        value_exprs = []
        for col in columns:
            if col in ("PIPELINE_EXECUTION_DETAILS", "STAGES_COUNT"):
                value_exprs.append(f"PARSE_JSON(%({col})s)")
            else:
                value_exprs.append(f"%({col})s")

        values_list_sql = ", ".join(value_exprs)

        insert_sql = f"""
        INSERT INTO {fully_qualified_table_name} (
            {column_list_sql}
        )
        SELECT
            {values_list_sql}
        """

        # -----------------------------------------------------------
        # Build params list (JSON-dump VARIANT columns)
        # -----------------------------------------------------------
        params_list: List[Dict[str, Any]] = []

        for rec_index, rec in enumerate(records):
            params: Dict[str, Any] = {}

            for col in columns:
                value = rec.get(col)

                if col in ("PIPELINE_EXECUTION_DETAILS", "STAGES_COUNT"):
                    if value is None:
                        params[col] = None
                    else:
                        params[col] = json.dumps(value)
                else:
                    params[col] = value

            params_list.append(params)

            log.debug(
                log_keyword,
                "Prepared parameters for a record.",
                RECORD_INDEX=rec_index,
            )

        # Try formatted preview
        try:
            format_and_print_sql_query(insert_sql, params_list[0])
        except Exception:
            log.debug(
                log_keyword,
                "SQL preview failed; continuing.",
            )

        # -----------------------------------------------------------
        # Bulk insert
        # -----------------------------------------------------------
        try:
            cursor.executemany(insert_sql, params_list)

            query_id = getattr(cursor, "sfqid", None)
            inserted_row_count = cursor.rowcount if cursor.rowcount is not None else len(records)

            result["query_id"] = query_id
            result["inserted_row_count"] = inserted_row_count

            log.info(
                log_keyword,
                "Bulk insert completed.",
                INSERTED_ROW_COUNT=inserted_row_count,
                QUERY_ID=query_id,
            )

            return result

        except Exception as exec_err:
            import traceback
            tb = traceback.format_exc()

            query_id = getattr(cursor, "sfqid", None)
            error_message = (
                f"[{log_keyword}] Failed to execute bulk insert on '{fully_qualified_table_name}'. "
                f"Error: {exec_err}"
            )

            result["continue_dag_run"] = False
            result["error_message"] = f"{error_message}\n{tb}"
            result["query_id"] = query_id

            log.error(
                log_keyword,
                "Exception during bulk insert.",
                error_message=error_message,
                QUERY_ID=query_id,
            )

            return result

    except Exception as e:
        import traceback
        tb = traceback.format_exc()

        error_message = (
            f"[{log_keyword}] Unexpected error in insert_multiple_records_into_tracking_table: {e}"
        )
        result["continue_dag_run"] = False
        result["error_message"] = f"{error_message}\n{tb}"

        log.error(
            log_keyword,
            "Unexpected exception in insert_multiple_records_into_tracking_table.",
            error_message=error_message,
        )
        return result

    finally:
        safe_close_cursor_and_conn(cursor, conn, log, log_keyword)

def fetch_last_recorded_window_end(
    config: Dict[str, Any],
    logger: CustomChainLogger,
    query_tag: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Fetch MAX(QUERY_WINDOW_END_TIMESTAMP) for a given PIPELINE_NAME + CONFIG_NAME
    from the tracking table, across ALL PIPELINE_STATUS values.

    Returns:
      {
        "continue_dag_run": bool,
        "error_message": Optional[str],
        "last_recorded_end": Optional[datetime],  # None if no rows
      }
    """
    log_keyword = "FETCH_LAST_RECORDED_WINDOW_END"
    log = logger.new_frame(log_keyword)

    result: Dict[str, Any] = {
        "continue_dag_run": False,
        "error_message": None,
        "last_recorded_end": None,
    }

    conn = None
    cursor = None

    try:
        pipeline_name = config["pipeline_name"]
        config_name = config["config_name"]

        sf_con_parms: Dict[str, Any] = config["sf_config_params"]
        table_config: Dict[str, Any] = config["pipeline_run_tracking_details"]

        database = table_config["database_name"]
        schema = table_config["schema_name"]
        table = table_config["table_name"]

        three_part_name = f"{database}.{schema}.{table}"

        log.info(
            log_keyword,
            "Fetching last recorded window end (all statuses).",
            PIPELINE_NAME=pipeline_name,
            CONFIG_NAME=config_name,
            TRACKING_TABLE=three_part_name,
            QUERY_TAG=query_tag,
        )

        conn_result = get_snowflake_connection(sf_con_parms, log, QUERY_TAG=query_tag)
        if not conn_result.get("continue_dag_run", False):
            error_message = (
                f"[{log_keyword}] Failed to obtain Snowflake connection. "
                f"Underlying error: {conn_result.get('error_message')}"
            )
            result["error_message"] = error_message
            log.error(log_keyword, error_message)
            return result

        conn = conn_result.get("conn")
        if conn is None:
            error_message = f"[{log_keyword}] Connection object is None."
            result["error_message"] = error_message
            log.error(log_keyword, error_message)
            return result

        cursor = conn.cursor(DictCursor)

        select_sql = f"""
        SELECT
            MAX(QUERY_WINDOW_END_TIMESTAMP) AS LAST_RECORDED_END
        FROM {three_part_name}
        WHERE PIPELINE_NAME = %(PIPELINE_NAME)s
          AND CONFIG_NAME = %(CONFIG_NAME)s
        """

        params = {
            "PIPELINE_NAME": pipeline_name,
            "CONFIG_NAME": config_name,
        }

        try:
            format_and_print_sql_query(select_sql, params)
        except Exception:
            log.debug(
                log_keyword,
                "format_and_print_sql_query failed; continuing with raw SQL.",
            )

        try:
            cursor.execute(select_sql, params)
            qid = getattr(cursor, "sfqid", None)
        except Exception as exec_err:
            import traceback
            tb = traceback.format_exc()
            qid = getattr(cursor, "sfqid", None) if cursor is not None else None
            error_message = (
                f"[{log_keyword}] SQL execution failed while fetching last recorded window end: {exec_err}"
            )
            result["error_message"] = f"{error_message}\n{tb}"
            log.error(
                log_keyword,
                "SQL execution error.",
                error_message=error_message,
                QUERY_ID=qid,
            )
            return result

        try:
            row = cursor.fetchone()
        except Exception as fetch_err:
            import traceback
            tb = traceback.format_exc()
            qid = getattr(cursor, "sfqid", None) if cursor is not None else None
            error_message = (
                f"[{log_keyword}] Failed to fetch row for last recorded window end: {fetch_err}"
            )
            result["error_message"] = f"{error_message}\n{tb}"
            log.error(
                log_keyword,
                "Row fetch error.",
                error_message=error_message,
                QUERY_ID=qid,
            )
            return result

        last_recorded_end = None
        if row:
            last_recorded_end = row.get("LAST_RECORDED_END")

        result["last_recorded_end"] = last_recorded_end
        result["continue_dag_run"] = True

        log.info(
            log_keyword,
            "Fetched last recorded window end.",
            LAST_RECORDED_END=str(last_recorded_end) if last_recorded_end else None,
        )

        return result

    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        error_message = f"[{log_keyword}] Unexpected failure in fetch_last_recorded_window_end: {e}"
        result["error_message"] = f"{error_message}\n{tb}"
        log.error(
            log_keyword,
            "Unhandled exception.",
            error_message=error_message,
        )
        return result

    finally:
        safe_close_cursor_and_conn(cursor, conn, log, log_keyword)


def get_next_n_records_for_data_pipeline(
    config: Dict[str, Any],
    logger: CustomChainLogger,
    query_tag: Optional[str] = None,
) -> Dict[str, Any]:
    """
    High-level orchestrator used by the downstream team.

    Steps:
      1) Fill any time gap in the tracking table by creating new PENDING records
         (WITHOUT duplicating existing rows) using:
             fill_gap_and_insert_pending_records_for_tracking_table().
      2) Fetch up to N PENDING records for this pipeline/config using:
             fetch_pending_records().
         Here N is driven by config["MAX_PENDING_RECORDS"] (or whatever
         fetch_pending_records expects).

    This function does NOT raise exceptions. All errors are folded into the
    return structure.

    Return structure (fixed as requested):
      {
          "continue_dag_run": bool,
          "error_message": Optional[str],

          # what downstream actually needs:
          "records": List[Dict[str, Any]],   # up to N PENDING records (existing + new)

          # optional extras:
          "pending_count_before": Optional[int],
          "new_records_created": Optional[int],
          "total_returned": int,
      }
    """
    log_keyword = "GET_NEXT_N_RECORDS_FOR_DATA_PIPELINE"
    log = logger.new_frame(log_keyword)

    result: Dict[str, Any] = {
        "continue_dag_run": True,
        "error_message": None,
        "records": [],
        "pending_count_before": None,  
        "new_records_created": None,
        "total_returned": 0,
    }

    try:
        log.info(
            log_keyword,
            "Starting orchestrator to get next N records for data pipeline.",
            QUERY_TAG=query_tag,
        )

        # ------------------------------------------------------------------
        # 1) Fill the gap FIRST (if any)
        # ------------------------------------------------------------------
        gap_fill_res = fill_gap_and_insert_pending_records_for_tracking_table(
            config=config,
            logger=log,
            query_tag=query_tag,
        )

        if not gap_fill_res.get("continue_dag_run", False):
            error_message = gap_fill_res.get(
                "error_message",
                "[GET_NEXT_N_RECORDS_FOR_DATA_PIPELINE] "
                "fill_gap_and_insert_pending_records_for_tracking_table failed.",
            )
            result["continue_dag_run"] = False
            result["error_message"] = error_message
            # We still capture how many new records were *attempted* to be created
            result["new_records_created"] = gap_fill_res.get("inserted_row_count", 0)

            log.error(
                log_keyword,
                "Gap-fill orchestrator returned continue_dag_run=False.",
                error_message=error_message,
                new_records_created=result["new_records_created"],
            )
            return result

        # If gap-fill succeeded, record how many new rows were inserted
        new_records_created = gap_fill_res.get("inserted_row_count", 0)
        result["new_records_created"] = int(new_records_created or 0)

        log.info(
            log_keyword,
            "Gap-fill step completed successfully.",
            new_records_created=result["new_records_created"],
        )

        # ------------------------------------------------------------------
        # 2) Fetch up to N PENDING records from tracking table
        # ------------------------------------------------------------------
        pending_res = fetch_pending_records(
            config=config,
            logger=log,
            query_tag=query_tag,
        )

        if not pending_res.get("continue_dag_run", False):
            error_message = pending_res.get(
                "error_message",
                "[GET_NEXT_N_RECORDS_FOR_DATA_PIPELINE] fetch_pending_records failed.",
            )
            result["continue_dag_run"] = False
            result["error_message"] = error_message
            # No records returned in failure case
            result["records"] = []
            result["total_returned"] = 0

            log.error(
                log_keyword,
                "fetch_pending_records returned continue_dag_run=False.",
                error_message=error_message,
            )
            return result

        records: List[Dict[str, Any]] = pending_res.get("records", []) or []
        result["records"] = records
        result["total_returned"] = len(records)

        log.info(
            log_keyword,
            "Successfully fetched PENDING records for downstream pipeline.",
            total_returned=result["total_returned"],
            new_records_created=result["new_records_created"],
        )

        return result

    except Exception as e:
        tb = traceback.format_exc()
        error_message = (
            f"[{log_keyword}] Unexpected error in get_next_n_records_for_data_pipeline: {e}"
        )

        result["continue_dag_run"] = False
        result["error_message"] = f"{error_message}\n{tb}"
        result["records"] = []
        result["total_returned"] = 0

        log.error(
            log_keyword,
            "Unexpected exception in get_next_n_records_for_data_pipeline.",
            error_message=error_message,
        )

        return result



def get_next_n_records_for_data_pipeline_main(config: Dict[str, Any]):
    # Setup the base logger
    base_logger = setup_logger()
    
    # We are in main_func. Create the *first* log frame.
    logger = CustomChainLogger(base_logger).new_frame("get_next_n_records_for_data_pipeline_main")
    query_tag = config.get("index_group") + "_" + config.get("business_index_name")
    result = get_next_n_records_for_data_pipeline( config, logger, query_tag  )

    return result


