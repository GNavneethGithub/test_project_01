# my_main_project_folder/framework/monitoring_scripts/sf_pipeline_tracking_scripts.py


import traceback
from typing import Dict, Any, List, Optional
from snowflake.connector import DictCursor
from utils.custom_logging import CustomChainLogger, setup_logger
from utils.db_utils import safe_close_cursor_and_conn
from utils.sql_formatter_utility import format_and_print_sql_query
from framework.monitoring_scripts.snowflake_connector import get_snowflake_connection


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
    log = logger.new_frame("create_pipeline_run_tracking_table_if_not_exists")

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
        log.info("PIPELINE_RUN_TRACKING_TABLE_CREATION", "Starting table creation flow.", query_tag=query_tag)

        sf_con_parms = config.get("sf_config_params", {}) or {}
        table_config = config.get("pipeline_run_tracking_details", {}) or {}

        database = table_config.get("database_name") or sf_con_parms.get("database") or "UNKNOWN_DB"
        schema = table_config.get("schema_name") or sf_con_parms.get("schema") or "PUBLIC"
        table = table_config.get("table_name") or "PIPELINE_RUN_TRACKING"

        three_part_name = f"{database}.{schema}.{table}"
        return_object["table_name"] = three_part_name

        log.info(
            "PIPELINE_RUN_TRACKING_TABLE_CREATION",
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
            log.error("PIPELINE_RUN_TRACKING_TABLE_CREATION", "Failed to get Snowflake connection.", error_message=msg)
            return_object["error_message"] = msg
            return return_object

        conn = conn_result.get("conn")
        if conn is None:
            msg = "Snowflake connector returned no connection object"
            log.error("PIPELINE_RUN_TRACKING_TABLE_CREATION", msg)
            return_object["error_message"] = msg
            return return_object

        cursor = conn.cursor()

        # ---------------------------------------------------------------------
        #  FINAL CREATE TABLE DDL (explicit defaults + no CREATED_BY)
        # ---------------------------------------------------------------------
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {three_part_name} (
            TRACKING_ID                     VARCHAR DEFAULT NULL,
            PIPELINE_NAME                   VARCHAR DEFAULT NULL,
            CONFIG_NAME                     VARCHAR DEFAULT NULL,
            CONFIG_VERSION                  FLOAT   DEFAULT NULL,

            QUERY_WINDOW_START_TIMESTAMP    TIMESTAMP_TZ DEFAULT NULL,
            QUERY_WINDOW_END_TIMESTAMP      TIMESTAMP_TZ DEFAULT NULL,

            QUERY_WINDOW_DAY                DATE    DEFAULT NULL,
            QUERY_WINDOW_DURATION           VARCHAR DEFAULT 'XX-YY',

            PIPELINE_EXECUTION_DETAILS      VARIANT DEFAULT NULL,

            SOURCE_COUNT                    INT     DEFAULT NULL,
            STAGES_COUNT                    VARIANT DEFAULT NULL,
            TARGET_COUNT                    INT     DEFAULT NULL,

            PIPELINE_STATUS                 VARCHAR DEFAULT 'PENDING',

            CREATED_AT                      TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
            UPDATED_AT                      TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
        )
        """

        log.info("PIPELINE_RUN_TRACKING_TABLE_CREATION", "Executing CREATE TABLE IF NOT EXISTS.")

        try:
            format_and_print_sql_query(create_table_sql, {})
        except Exception:
            log.warning(
                "PIPELINE_RUN_TRACKING_TABLE_CREATION",
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
                "PIPELINE_RUN_TRACKING_TABLE_CREATION",
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
                "PIPELINE_RUN_TRACKING_TABLE_CREATION",
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
            "PIPELINE_RUN_TRACKING_TABLE_CREATION",
            "Unexpected exception.",
            error_message=msg,
            query_id=qid,
        )

        return_object["error_message"] = f"{msg}\n{tb}"
        return_object["continue_dag_run"] = False
        return return_object

    finally:
        safe_close_cursor_and_conn(cursor, conn, log, "PIPELINE_RUN_TRACKING_TABLE_CREATION")

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
    log = logger.new_frame("fetch_pipeline_run_by_tracking_id")

    return_object = {
        "continue_dag_run": False,
        "error_message": None,
        "query_id": None,
        "records": []
    }

    conn = None
    cursor = None

    try:
        log.info("PIPELINE_RUN_TRACKING_FETCH", "Starting fetch by TRACKING_ID.", tracking_id=tracking_id)

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
                "PIPELINE_RUN_TRACKING_FETCH",
                "Failed to get Snowflake connection.",
                error_message=conn_result.get("error_message")
            )
            return_object["error_message"] = conn_result.get("error_message")
            return return_object

        conn = conn_result.get("conn")
        if conn is None:
            msg = "Snowflake connector returned no connection object"
            log.error("PIPELINE_RUN_TRACKING_FETCH", msg)
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
            log.debug("PIPELINE_RUN_TRACKING_FETCH", "SQL formatter failed; proceeding with raw SQL.")

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
                "PIPELINE_RUN_TRACKING_FETCH",
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
            log.error("PIPELINE_RUN_TRACKING_FETCH", "Failed to fetch rows.", error_message=msg, query_id=qid)
            return_object["error_message"] = f"{msg}\n{tb}"
            return_object["query_id"] = qid
            return return_object

        return_object["records"] = rows
        log.info("PIPELINE_RUN_TRACKING_FETCH", "Fetch executed.", query_id=return_object["query_id"], record_count=len(rows))

        return_object["continue_dag_run"] = True
        return return_object

    except Exception as e:
        tb = traceback.format_exc()
        qid = getattr(cursor, "sfqid", None) if cursor is not None else None
        err_msg = f"Fetch failed: {str(e)}"
        log.error("PIPELINE_RUN_TRACKING_FETCH", "Exception during fetch.", exc_info=True, error_message=str(e), query_id=qid)
        return_object["error_message"] = f"{err_msg}\n{tb}"
        return_object["query_id"] = qid
        return return_object

    finally:
        # use unified cleanup helper
        safe_close_cursor_and_conn(cursor, conn, log, "PIPELINE_RUN_TRACKING_FETCH")

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
      - CONFIG_VERSION       : Optional[float|str]
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
      - All logs use the exact keyword "FETCH_PENDING_RECORDS"
      - Info: concise operational summary (no preview)
      - Warning: recoverable / noteworthy conditions
      - Error: fatal to this function
      - Debug: as much useful internal state as possible (including full_rows)
    """
    log = logger.new_frame("fetch_pending_records")

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
        log.info("FETCH_PENDING_RECORDS", "Starting fetch of pending pipeline runs.", query_tag=query_tag)

        # Minimal config reads (uppercase-only keys)
        if "MAX_PENDING_RECORDS" not in config:
            msg = "config must include 'MAX_PENDING_RECORDS' (uppercase key required)"
            log.error("FETCH_PENDING_RECORDS", msg)
            result["error_message"] = msg
            return result

        try:
            MAX_PENDING_RECORDS = int(config["MAX_PENDING_RECORDS"])
        except Exception:
            msg = f"MAX_PENDING_RECORDS must be an integer; got: {config.get('MAX_PENDING_RECORDS')!r}"
            log.error("FETCH_PENDING_RECORDS", msg)
            result["error_message"] = msg
            return result

        # Read optional uppercase filters only (no lowercase fallbacks)
        PIPELINE_NAME = config.get("PIPELINE_NAME")
        CONFIG_NAME = config.get("CONFIG_NAME")
        CONFIG_VERSION = config.get("CONFIG_VERSION")

        # Debug: show resolved inputs (safe; do not include secrets)
        log.debug(
            "FETCH_PENDING_RECORDS",
            "Resolved inputs",
            inputs={
                "MAX_PENDING_RECORDS": MAX_PENDING_RECORDS,
                "PIPELINE_NAME": PIPELINE_NAME,
                "CONFIG_NAME": CONFIG_NAME,
                "CONFIG_VERSION": CONFIG_VERSION,
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
            "FETCH_PENDING_RECORDS",
            "Resolved Snowflake target and initial settings.",
            target_table=three_part_name,
            requested_limit=MAX_PENDING_RECORDS
        )

        # Obtain connection via helper
        conn_result = get_snowflake_connection(sf_con_parms, log, QUERY_TAG=query_tag)
        if not conn_result.get("continue_dag_run"):
            msg = conn_result.get("error_message") or "Failed to obtain Snowflake connection"
            log.error("FETCH_PENDING_RECORDS", "Failed to get Snowflake connection.", error_message=msg)
            result["error_message"] = msg
            return result

        conn = conn_result.get("conn")
        if conn is None:
            msg = "Snowflake connector returned no connection object"
            log.error("FETCH_PENDING_RECORDS", msg)
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
        if CONFIG_VERSION is not None:
            where_clauses.append("CONFIG_VERSION = %(CONFIG_VERSION)s")
            params["CONFIG_VERSION"] = CONFIG_VERSION

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
            log.debug("FETCH_PENDING_RECORDS", "Final SQL prepared for execution", sql=select_sql, params=params)
        except Exception:
            log.warning("FETCH_PENDING_RECORDS", "format_and_print_sql_query failed; proceeding without formatted print")
            log.debug("FETCH_PENDING_RECORDS", "Raw SQL", sql=select_sql, params=params)

        # Execute query
        try:
            cursor.execute(select_sql, params)
            # safe direct access on success
            result["query_id"] = cursor.sfqid
        except Exception as exec_err:
            tb = traceback.format_exc()
            msg = f"SQL execution failed: {str(exec_err)}"
            qid = getattr(cursor, "sfqid", None)
            log.error("FETCH_PENDING_RECORDS", "Failed to execute SELECT on tracking table.", error_message=msg, query_id=qid)
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
            log.error("FETCH_PENDING_RECORDS", "Failed to fetch rows.", error_message=msg, query_id=qid)
            result["error_message"] = f"{msg}\n{tb}"
            result["query_id"] = qid
            result["continue_dag_run"] = False
            return result

        result["records"] = rows
        result["continue_dag_run"] = True

        # Info-level: concise summary (no preview)
        log.info(
            "FETCH_PENDING_RECORDS",
            "Fetched pending pipeline runs.",
            query_id=result["query_id"],
            requested_limit=MAX_PENDING_RECORDS,
            returned_count=len(rows)
        )

        # Debug: full rows (detailed preview only at debug level)
        try:
            log.debug("FETCH_PENDING_RECORDS", "Full fetched rows (debug)", full_rows=rows)
        except Exception:
            log.debug("FETCH_PENDING_RECORDS", "Could not serialize full rows in debug log.", exc_info=True)

        return result

    except Exception as e:
        tb = traceback.format_exc()
        err_msg = f"Unexpected failure in fetch_pending_records: {str(e)}"
        log.error("FETCH_PENDING_RECORDS", "Unhandled exception in fetch_pending_records.", error_message=err_msg)
        result["error_message"] = f"{err_msg}\n{tb}"
        result["continue_dag_run"] = False
        return result

    finally:
        # unified cleanup helper
        safe_close_cursor_and_conn(cursor, conn, log, "FETCH_PENDING_RECORDS")

def fetch_running_records(
    config: Dict[str, Any],
    logger: CustomChainLogger,
    query_tag: Optional[str] = None
) -> Dict[str, Any]:
    """
    Fetch all records with PIPELINE_STATUS = 'RUNNING', applying optional filters:
      - PIPELINE_NAME (uppercase key)
      - CONFIG_NAME   (uppercase key)
      - CONFIG_VERSION(uppercase key)

    Returns (always, never raises):
      {
        "continue_dag_run": bool,
        "error_message": Optional[str],
        "query_id": Optional[str],
        "records": List[Dict]
      }

    Logging keyword (immutable): "FETCH_RUNNING_RECORDS"
    """
    log = logger.new_frame("fetch_running_records")

    result: Dict[str, Any] = {
        "continue_dag_run": False,
        "error_message": None,
        "query_id": None,
        "records": []
    }

    conn = None
    cursor = None

    try:
        log.info("FETCH_RUNNING_RECORDS", "Starting fetch of RUNNING pipeline records.", query_tag=query_tag)

        # Read uppercase-only filters
        PIPELINE_NAME = config.get("PIPELINE_NAME")
        CONFIG_NAME = config.get("CONFIG_NAME")
        CONFIG_VERSION = config.get("CONFIG_VERSION")

        # Debug: resolved filters
        log.debug(
            "FETCH_RUNNING_RECORDS",
            "Resolved inputs for fetch_running_records",
            inputs={
                "PIPELINE_NAME": PIPELINE_NAME,
                "CONFIG_NAME": CONFIG_NAME,
                "CONFIG_VERSION": CONFIG_VERSION,
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

        log.info("FETCH_RUNNING_RECORDS", "Resolved Snowflake target for running fetch.", target_table=three_part_name)

        # Obtain connection
        conn_result = get_snowflake_connection(sf_con_parms, log, QUERY_TAG=query_tag)
        if not conn_result.get("continue_dag_run"):
            msg = conn_result.get("error_message") or "Failed to obtain Snowflake connection"
            log.error("FETCH_RUNNING_RECORDS", "Failed to get Snowflake connection.", error_message=msg)
            result["error_message"] = msg
            return result

        conn = conn_result.get("conn")
        if conn is None:
            msg = "Snowflake connector returned no connection object"
            log.error("FETCH_RUNNING_RECORDS", msg)
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
        if CONFIG_VERSION is not None:
            where_clauses.append("CONFIG_VERSION = %(CONFIG_VERSION)s")
            params["CONFIG_VERSION"] = CONFIG_VERSION

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
            log.debug("FETCH_RUNNING_RECORDS", "Final SQL prepared for execution", sql=select_sql, params=params)
        except Exception:
            log.debug("FETCH_RUNNING_RECORDS", "format_and_print_sql_query failed; proceeding with raw SQL", sql=select_sql, params=params)

        # Execute query
        try:
            cursor.execute(select_sql, params)
            # safe direct access on success
            result["query_id"] = cursor.sfqid
        except Exception as exec_err:
            tb = traceback.format_exc()
            msg = f"SQL execution failed: {str(exec_err)}"
            qid = getattr(cursor, "sfqid", None)
            log.error("FETCH_RUNNING_RECORDS", "Failed to execute SELECT on tracking table.", error_message=msg, query_id=qid)
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
            log.error("FETCH_RUNNING_RECORDS", "Failed to fetch rows.", error_message=msg, query_id=qid)
            result["error_message"] = f"{msg}\n{tb}"
            result["query_id"] = qid
            result["continue_dag_run"] = False
            return result

        result["records"] = rows
        result["continue_dag_run"] = True

        # Info: concise summary
        log.info(
            "FETCH_RUNNING_RECORDS",
            "Fetched RUNNING pipeline records.",
            query_id=result["query_id"],
            returned_count=len(rows)
        )

        # Debug: full rows
        try:
            log.debug("FETCH_RUNNING_RECORDS", "Full fetched RUNNING rows (debug)", full_rows=rows)
        except Exception:
            log.debug("FETCH_RUNNING_RECORDS", "Could not serialize full rows in debug log.", exc_info=True)

        return result

    except Exception as e:
        tb = traceback.format_exc()
        err_msg = f"Unexpected failure in fetch_running_records: {str(e)}"
        qid = getattr(cursor, "sfqid", None) if cursor is not None else None
        log.error("FETCH_RUNNING_RECORDS", "Unhandled exception in fetch_running_records.", error_message=err_msg, query_id=qid)
        result["error_message"] = f"{err_msg}\n{tb}"
        result["continue_dag_run"] = False
        result["query_id"] = qid
        return result

    finally:
        safe_close_cursor_and_conn(cursor, conn, log, "FETCH_RUNNING_RECORDS")

def update_tracking_record_partial(
    config: Dict[str, Any],
    logger: CustomChainLogger,
    record: Dict[str, Any],
    query_tag: Optional[str] = None
) -> Dict[str, Any]:
    """
    Partially update a single tracking row identified by TRACKING_ID.
    """
    log = logger.new_frame("update_tracking_record_partial")
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
        "CONFIG_VERSION",
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
            "UPDATE_TRACKING_RECORD_PARTIAL",
            "Starting partial update for tracking record.",
            tracking_id=record.get("TRACKING_ID"),
            query_tag=query_tag
        )

        # TRACKING_ID required
        TRACKING_ID = record.get("TRACKING_ID")
        if not TRACKING_ID:
            msg = "record must include TRACKING_ID (uppercase key required)"
            log.error("UPDATE_TRACKING_RECORD_PARTIAL", msg)
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
            log.error("UPDATE_TRACKING_RECORD_PARTIAL", "Failed to get Snowflake connection.", error_message=msg)
            result["error_message"] = msg
            return result

        conn = conn_result.get("conn")
        if conn is None:
            msg = "Snowflake connector returned no connection object"
            log.error("UPDATE_TRACKING_RECORD_PARTIAL", msg)
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
            log.debug("UPDATE_TRACKING_RECORD_PARTIAL", "Prepared UPDATE", sql=update_sql, params=params, full_record=record)
        except Exception:
            log.debug("UPDATE_TRACKING_RECORD_PARTIAL", "Prepared UPDATE (raw)", sql=update_sql, params=params)

        # Execute update
        try:
            cursor.execute(update_sql, params)
            qid = cursor.sfqid
            result["query_id"] = qid
            log.info("UPDATE_TRACKING_RECORD_PARTIAL", "UPDATE executed", tracking_id=TRACKING_ID, query_id=qid)
        except Exception as upd_err:
            tb = traceback.format_exc()
            qid = getattr(cursor, "sfqid", None)
            msg = f"Failed to execute UPDATE for TRACKING_ID={TRACKING_ID}: {str(upd_err)}"
            log.error("UPDATE_TRACKING_RECORD_PARTIAL", "Update failed.", error_message=msg, query_id=qid)
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

        err_msg = f"Unexpected failure in update_tracking_record_partial: {str(e)}"
        log.error("UPDATE_TRACKING_RECORD_PARTIAL", "Unhandled exception in update_tracking_record_partial.",
                  error_message=err_msg, query_id=qid_fallback)
        result["error_message"] = f"{err_msg}\n{tb}"
        result["continue_dag_run"] = False
        result["query_id"] = qid_fallback
        return result

    finally:
        safe_close_cursor_and_conn(cursor, conn, log, "UPDATE_TRACKING_RECORD_PARTIAL")











