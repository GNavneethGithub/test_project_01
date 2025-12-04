# framework/time_context_script.py

from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore

from utils.custom_logging import CustomChainLogger
from utils.time_utils import (
    get_timestamp_floor,
    parse_duration_string,
    convert_timedelta_to_duration_string,
)

from framework.monitoring_scripts.sf_pipeline_tracking_scripts import (
    fetch_last_recorded_window_end,
    insert_multiple_records_into_tracking_table
)



def compute_time_context(
    config: Dict[str, Any],
    logger: CustomChainLogger,
) -> Dict[str, Any]:
    """
    Purpose:
        Determine the correct candidate_start timestamp to begin creating windows.

    Returns:
        {
            "continue_dag_run": bool,
            "error_message": Optional[str],
            "candidate_start": Optional[datetime],
        }
    """
    log_keyword = "COMPUTE_TIME_CONTEXT_FOR_NEXT_RECORDS"
    log = logger.new_frame(log_keyword)

    result = {
        "continue_dag_run": True,
        "error_message": None,
        "candidate_start": None,
    }

    try:
        tz_name: str = config["timezone"]
        query_window_str: str = config["query_interval_window"]
        freshness_window_str: str = config["data_freshness_block_window"]

        now_utc = datetime.now(timezone.utc)
        now_local = now_utc.astimezone(ZoneInfo(tz_name))

        log.info(
            log_keyword,
            "Starting _compute_time_context",
            timezone=tz_name,
            now_local=str(now_local),
            query_interval_window=query_window_str,
            data_freshness_block_window=freshness_window_str,
        )

        # 1) Parse duration strings
        q_res = parse_duration_string(query_window_str, log)
        if not q_res["continue_dag_run"]:
            err = f"[{log_keyword}] Failed to parse query_interval_window='{query_window_str}'. {q_res['error_message']}"
            result["continue_dag_run"] = False
            result["error_message"] = err
            log.error(log_keyword, err)
            return result

        d_res = parse_duration_string(freshness_window_str, log)
        if not d_res["continue_dag_run"]:
            err = f"[{log_keyword}] Failed to parse data_freshness_block_window='{freshness_window_str}'. {d_res['error_message']}"
            result["continue_dag_run"] = False
            result["error_message"] = err
            log.error(log_keyword, err)
            return result

        # 2) Combine deltas
        combined_delta = q_res["timedelta"] + d_res["timedelta"]

        # 3) Convert to duration string
        conv_res = convert_timedelta_to_duration_string(combined_delta, log)
        if not conv_res["continue_dag_run"]:
            err = (
                f"[{log_keyword}] Failed to convert combined timedelta='{combined_delta}' "
                f"to duration string. {conv_res['error_message']}"
            )
            result["continue_dag_run"] = False
            result["error_message"] = err
            log.error(log_keyword, err)
            return result

        combined_duration_str = conv_res["duration_string"]

        log.info(
            log_keyword,
            "Combined duration string created",
            combined_duration_str=combined_duration_str,
        )

        # 4) Floor to compute candidate_start
        floor_res = get_timestamp_floor(
            dt=now_local,
            duration_str=combined_duration_str,
            logger=log,
        )

        if not floor_res["continue_dag_run"]:
            err = (
                f"[{log_keyword}] get_timestamp_floor failed for duration_str='{combined_duration_str}'. "
                f"{floor_res['error_message']}"
            )
            result["continue_dag_run"] = False
            result["error_message"] = err
            log.error(log_keyword, err)
            return result

        candidate_start = floor_res["floor_datetime"]
        if candidate_start is None:
            err = (
                f"[{log_keyword}] floor_datetime=None for duration_str='{combined_duration_str}'."
            )
            result["continue_dag_run"] = False
            result["error_message"] = err
            log.error(log_keyword, err)
            return result

        # 5) Final output
        result["candidate_start"] = candidate_start

        log.info(
            log_keyword,
            "Successfully computed candidate_start",
            candidate_start=str(candidate_start),
        )

        return result

    except Exception as e:
        err = f"[{log_keyword}] Unexpected error: {e}"
        result["continue_dag_run"] = False
        result["error_message"] = err
        log.error(log_keyword, err)
        return result


def build_pending_tracking_record_for_start_timestamp(
    query_window_start_timestamp: datetime,
    config: Dict[str, Any],
    logger: CustomChainLogger,
) -> Dict[str, Any]:
    """
    Build a single tracking-table-style record (Python dict) for a given
    QUERY_WINDOW_START_TIMESTAMP, with PIPELINE_STATUS='PENDING'.

    This function DOES NOT insert into Snowflake. It only prepares the
    record dict that can later be used by an insert function.

    Expected config keys (assumed to exist):
      - "pipeline_name": str
      - "config_name": str
      - "query_interval_window": str   (e.g. "1h")

    Return structure:
      {
          "continue_dag_run": bool,
          "error_message": Optional[str],
          "record": Optional[Dict[str, Any]],
      }
    """
    log_keyword = "BUILD_PENDING_TRACKING_RECORD_FOR_START_TIMESTAMP"
    log = logger.new_frame(log_keyword)

    result: Dict[str, Any] = {
        "continue_dag_run": True,
        "error_message": None,
        "record": None,
    }

    try:
        pipeline_name: str = config["pipeline_name"]
        config_name: str = config["config_name"]
        query_interval_window: str = config["query_interval_window"]

        log.info(
            log_keyword,
            "Building pending tracking record for given start timestamp",
            PIPELINE_NAME=pipeline_name,
            CONFIG_NAME=config_name,
            QUERY_WINDOW_START_TIMESTAMP=str(query_window_start_timestamp),
            QUERY_INTERVAL_WINDOW=query_interval_window,
        )

        # 1) Parse the query interval window duration (e.g. "1h")
        duration_res = parse_duration_string(query_interval_window, log)
        if not duration_res.get("continue_dag_run", False):
            underlying_error = duration_res.get("error_message", "Unknown error from parse_duration_string")
            error_message = (
                f"[{log_keyword}] Failed to parse QUERY_INTERVAL_WINDOW='{query_interval_window}'. "
                f"Underlying error: {underlying_error}"
            )
            result["continue_dag_run"] = False
            result["error_message"] = error_message

            log.error(
                log_keyword,
                "parse_duration_string failed for QUERY_INTERVAL_WINDOW",
                QUERY_INTERVAL_WINDOW=query_interval_window,
                underlying_error=underlying_error,
            )
            return result

        delta = duration_res.get("timedelta")
        if delta is None or delta.total_seconds() <= 0:
            error_message = (
                f"[{log_keyword}] Invalid timedelta parsed from QUERY_INTERVAL_WINDOW='{query_interval_window}'. "
                f"Parsed value: {delta}"
            )
            result["continue_dag_run"] = False
            result["error_message"] = error_message

            log.error(
                log_keyword,
                "Invalid timedelta for QUERY_INTERVAL_WINDOW",
                QUERY_INTERVAL_WINDOW=query_interval_window,
                parsed_timedelta=str(delta),
            )
            return result

        # 2) Compute end timestamp and window day
        query_window_end_timestamp = query_window_start_timestamp + delta
        query_window_day = query_window_start_timestamp.date()

        # 3) Build PIPELINE_EXECUTION_DETAILS default structure (ALL KEYS UPPERCASE)
        pipeline_execution_details: Dict[str, Any] = {
            "PIPELINE_STARTED_AT": None,
            "PIPELINE_ENDED_AT": None,
            "PIPELINE_MAX_RUN_THRESHOLD_TIMESTAMP": None,
            "PIPELINE_STATUS": None,

            "RUNNING_PHASE": None,

            "PENDING_PHASES": [],
            "USER_REQUESTED_TO_SKIP_PHASES": [],
            "COMPLETED_PHASES": [],

            "RETRY_ATTEMPT_NUMBER": None,
        }

        # 4) Build the record dict following tracking_tbl schema (ALL COLUMN NAMES UPPERCASE)
        record: Dict[str, Any] = {
            "TRACKING_ID": None,

            "PIPELINE_NAME": pipeline_name,
            "CONFIG_NAME": config_name,

            "QUERY_WINDOW_START_TIMESTAMP": query_window_start_timestamp,
            "QUERY_WINDOW_END_TIMESTAMP": query_window_end_timestamp,

            "QUERY_WINDOW_DAY": query_window_day,
            "QUERY_WINDOW_DURATION": query_interval_window,

            "PIPELINE_EXECUTION_DETAILS": pipeline_execution_details,

            "SOURCE_COUNT": None,
            "STAGES_COUNT": None,
            "TARGET_COUNT": None,

            "PIPELINE_STATUS": "PENDING",

            # Let Snowflake DEFAULTs populate these on insert
            "CREATED_AT": None,
            "UPDATED_AT": None,
        }

        result["record"] = record

        log.info(
            log_keyword,
            "Successfully built pending tracking record",
            PIPELINE_NAME=pipeline_name,
            CONFIG_NAME=config_name,
            QUERY_WINDOW_START_TIMESTAMP=str(query_window_start_timestamp),
            QUERY_WINDOW_END_TIMESTAMP=str(query_window_end_timestamp),
            QUERY_WINDOW_DAY=str(query_window_day),
        )

        return result

    except Exception as e:
        error_message = (
            f"[{log_keyword}] Unexpected error while building pending tracking record: {e}"
        )
        result["continue_dag_run"] = False
        result["error_message"] = error_message

        log.error(
            log_keyword,
            "Unexpected exception in build_pending_tracking_record_for_start_timestamp",
            exception=str(e),
            error_message=error_message,
        )

        return result


def get_query_window_pairs_list(
    start_query_window_start_timestamp: datetime,
    end_query_window_start_timestamp: datetime,
    config: Dict[str, Any],
    logger: CustomChainLogger,
) -> Dict[str, Any]:
    """
    Generate a list of contiguous query-window (start, end) pairs between
    start_query_window_start_timestamp and end_query_window_start_timestamp.

    This function ONLY computes window boundaries.
    Does NOT build tracking records.
    Does NOT insert into Snowflake.

    Returns:
      {
          "continue_dag_run": bool,
          "error_message": Optional[str],
          "list_of_query_windows": List[Dict[str, Any]],
      }
    """
    log_keyword = "GET_QUERY_WINDOW_PAIRS_LIST"
    log = logger.new_frame(log_keyword)

    result: Dict[str, Any] = {
        "continue_dag_run": True,
        "error_message": None,
        "list_of_query_windows": [],
    }

    try:
        # No gap → no windows
        if start_query_window_start_timestamp >= end_query_window_start_timestamp:
            log.info(
                log_keyword,
                "No gap detected. Returning empty list_of_query_windows.",
                START_QUERY_WINDOW_START_TIMESTAMP=str(start_query_window_start_timestamp),
                END_QUERY_WINDOW_START_TIMESTAMP=str(end_query_window_start_timestamp),
            )
            return result

        query_interval_window: str = config["query_interval_window"]

        log.info(
            log_keyword,
            "Starting to compute query window pairs.",
            START_QUERY_WINDOW_START_TIMESTAMP=str(start_query_window_start_timestamp),
            END_QUERY_WINDOW_START_TIMESTAMP=str(end_query_window_start_timestamp),
            QUERY_INTERVAL_WINDOW=query_interval_window,
        )

        # Parse the query window interval
        duration_res = parse_duration_string(query_interval_window, log)
        if not duration_res.get("continue_dag_run", False):
            underlying_error = duration_res.get("error_message", "Unknown error")
            error_message = (
                f"[{log_keyword}] Failed to parse QUERY_INTERVAL_WINDOW='{query_interval_window}'. "
                f"Underlying error: {underlying_error}"
            )
            result["continue_dag_run"] = False
            result["error_message"] = error_message
            log.error(log_keyword, error_message)
            return result

        interval_delta = duration_res.get("timedelta")
        if not isinstance(interval_delta, timedelta) or interval_delta.total_seconds() <= 0:
            error_message = (
                f"[{log_keyword}] Invalid timedelta parsed from '{query_interval_window}'. "
                f"Value: {interval_delta}"
            )
            result["continue_dag_run"] = False
            result["error_message"] = error_message
            log.error(log_keyword, error_message)
            return result

        # Build windows
        current_start = start_query_window_start_timestamp
        windows: List[Dict[str, Any]] = []

        while True:
            current_end = current_start + interval_delta
            if current_end > end_query_window_start_timestamp:
                break

            windows.append(
                {
                    "QUERY_WINDOW_START_TIMESTAMP": current_start,
                    "QUERY_WINDOW_END_TIMESTAMP": current_end,
                }
            )

            log.debug(
                log_keyword,
                "Added query window pair.",
                QUERY_WINDOW_START_TIMESTAMP=str(current_start),
                QUERY_WINDOW_END_TIMESTAMP=str(current_end),
            )

            current_start = current_end

        result["list_of_query_windows"] = windows

        log.info(
            log_keyword,
            "Completed computing query window pairs.",
            TOTAL_WINDOWS=len(windows),
            START_QUERY_WINDOW_START_TIMESTAMP=str(start_query_window_start_timestamp),
            END_QUERY_WINDOW_START_TIMESTAMP=str(end_query_window_start_timestamp),
        )

        return result

    except Exception as e:
        error_message = f"[{log_keyword}] Unexpected error: {e}"
        result["continue_dag_run"] = False
        result["error_message"] = error_message
        log.error(log_keyword, error_message)
        return result


def building_single_record_for_tracking_table(
    query_window_start_timestamp: datetime,
    query_window_end_timestamp: datetime,
    config: Dict[str, Any],
    logger: CustomChainLogger,
) -> Dict[str, Any]:
    """
    Build ONE complete tracking-table-style record (Python dict).

    This function does NOT insert into Snowflake.
    It only builds the in-memory dict that represents a row in the tracking table.

    Inputs:
      - query_window_start_timestamp : datetime
      - query_window_end_timestamp   : datetime
      - config                       : Dict[str, Any]
             required keys:
                * pipeline_name
                * config_name
                * query_interval_window
      - logger                       : CustomChainLogger

    Returns:
      {
          "continue_dag_run": bool,
          "error_message": Optional[str],
          "record": Optional[Dict[str, Any]],
      }
    """

    log_keyword = "BUILDING_SINGLE_RECORD_FOR_TRACKING_TABLE"
    log = logger.new_frame(log_keyword)

    result: Dict[str, Any] = {
        "continue_dag_run": True,
        "error_message": None,
        "record": None,
    }

    try:
        pipeline_name: str = config["pipeline_name"]
        config_name: str = config["config_name"]
        query_interval_window: str = config["query_interval_window"]

        log.info(
            log_keyword,
            "Building single tracking record.",
            PIPELINE_NAME=pipeline_name,
            CONFIG_NAME=config_name,
            QUERY_WINDOW_START_TIMESTAMP=str(query_window_start_timestamp),
            QUERY_WINDOW_END_TIMESTAMP=str(query_window_end_timestamp),
        )

        # Compute derived values
        query_window_day = query_window_start_timestamp.date()

        # Build PIPELINE_EXECUTION_DETAILS with all KEYS UPPERCASE
        pipeline_execution_details: Dict[str, Any] = {
            "PIPELINE_STARTED_AT": None,
            "PIPELINE_ENDED_AT": None,
            "PIPELINE_MAX_RUN_THRESHOLD_TIMESTAMP": None,
            "PIPELINE_STATUS": None,

            "RUNNING_PHASE": None,

            "PENDING_PHASES": ["PRE_VALIDATION", "SOURCE_TO_STAGE_TRANSFER", "STAGE_TO_TARGET_TRANSFER", "SOURCE_VS_STAGE_AUDIT", "SOURCE_VS_TARGET_AUDIT","STAGE_CLEANING","TARGET_CLEANING"],
            "USER_REQUESTED_TO_SKIP_PHASES": ["SOURCE_VS_STAGE_AUDIT"],
            "COMPLETED_PHASES": [],

            "RETRY_ATTEMPT_NUMBER": None,
        }

        # Build FULL record (ALL column names UPPERCASE)
        record: Dict[str, Any] = {
            "TRACKING_ID": None,

            "PIPELINE_NAME": pipeline_name,
            "CONFIG_NAME": config_name,

            "QUERY_WINDOW_START_TIMESTAMP": query_window_start_timestamp,
            "QUERY_WINDOW_END_TIMESTAMP": query_window_end_timestamp,

            "QUERY_WINDOW_DAY": query_window_day,
            "QUERY_WINDOW_DURATION": query_interval_window,

            "PIPELINE_EXECUTION_DETAILS": pipeline_execution_details,

            "SOURCE_COUNT": None,
            "STAGES_COUNT": None,
            "TARGET_COUNT": None,

            "PIPELINE_STATUS": "PENDING",

            "CREATED_AT": None,
            "UPDATED_AT": None,
        }

        result["record"] = record

        log.info(
            log_keyword,
            "Single tracking record built successfully.",
            QUERY_WINDOW_START_TIMESTAMP=str(query_window_start_timestamp),
            QUERY_WINDOW_END_TIMESTAMP=str(query_window_end_timestamp),
        )

        return result

    except Exception as e:
        error_message = (
            f"[{log_keyword}] Unexpected error while building single tracking record: {e}"
        )
        result["continue_dag_run"] = False
        result["error_message"] = error_message

        log.error(
            log_keyword,
            "Unexpected exception.",
            exception=str(e),
            error_message=error_message,
        )

        return result


def building_multiple_records_for_tracking_table_from_query_windows_list(
    list_of_query_windows: List[Dict[str, Any]],
    config: Dict[str, Any],
    logger: CustomChainLogger,
) -> Dict[str, Any]:
    """
    Build multiple tracking-table-style records (Python dicts) from a list of
    query window (start, end) pairs.

    This function:
      - DOES NOT insert into Snowflake.
      - ONLY builds the list of record dicts by calling
        building_single_record_for_tracking_table(...) for each window.

    Expected structure of list_of_query_windows:
      [
        {
          "QUERY_WINDOW_START_TIMESTAMP": datetime,
          "QUERY_WINDOW_END_TIMESTAMP": datetime,
        },
        ...
      ]

    Returns:
      {
          "continue_dag_run": bool,
          "error_message": Optional[str],
          "records": List[Dict[str, Any]],   # list of tracking-table-style records
      }
    """
    log_keyword = "BUILDING_MULTIPLE_RECORDS_FOR_TRACKING_TABLE_FROM_QUERY_WINDOWS_LIST"
    log = logger.new_frame(log_keyword)

    result: Dict[str, Any] = {
        "continue_dag_run": True,
        "error_message": None,
        "records": [],
    }

    try:
        if not list_of_query_windows:
            log.info(
                log_keyword,
                "Input list_of_query_windows is empty. Nothing to build.",
            )
            return result  # continue_dag_run=True, records=[]

        records: List[Dict[str, Any]] = []

        log.info(
            log_keyword,
            "Starting to build multiple tracking records from query windows list.",
            TOTAL_QUERY_WINDOWS=len(list_of_query_windows),
        )

        for index, window_info in enumerate(list_of_query_windows):
            query_window_start_timestamp: datetime = window_info["QUERY_WINDOW_START_TIMESTAMP"]
            query_window_end_timestamp: datetime = window_info["QUERY_WINDOW_END_TIMESTAMP"]

            log.debug(
                log_keyword,
                "Building single tracking record for query window.",
                INDEX=index,
                QUERY_WINDOW_START_TIMESTAMP=str(query_window_start_timestamp),
                QUERY_WINDOW_END_TIMESTAMP=str(query_window_end_timestamp),
            )

            single_res = building_single_record_for_tracking_table(
                query_window_start_timestamp=query_window_start_timestamp,
                query_window_end_timestamp=query_window_end_timestamp,
                config=config,
                logger=log,
            )

            if not single_res.get("continue_dag_run", False):
                error_message = single_res.get(
                    "error_message",
                    (
                        f"[{log_keyword}] building_single_record_for_tracking_table failed "
                        f"for INDEX={index}, START='{query_window_start_timestamp}', "
                        f"END='{query_window_end_timestamp}'"
                    ),
                )
                result["continue_dag_run"] = False
                result["error_message"] = error_message

                log.error(
                    log_keyword,
                    "Failed to build single tracking record from query window.",
                    INDEX=index,
                    QUERY_WINDOW_START_TIMESTAMP=str(query_window_start_timestamp),
                    QUERY_WINDOW_END_TIMESTAMP=str(query_window_end_timestamp),
                    error_message=error_message,
                )
                return result

            record = single_res.get("record")
            if record is None:
                error_message = (
                    f"[{log_keyword}] building_single_record_for_tracking_table returned "
                    f"no record for INDEX={index}, START='{query_window_start_timestamp}', "
                    f"END='{query_window_end_timestamp}'"
                )
                result["continue_dag_run"] = False
                result["error_message"] = error_message

                log.error(
                    log_keyword,
                    "No record returned from building_single_record_for_tracking_table.",
                    INDEX=index,
                    QUERY_WINDOW_START_TIMESTAMP=str(query_window_start_timestamp),
                    QUERY_WINDOW_END_TIMESTAMP=str(query_window_end_timestamp),
                )
                return result

            records.append(record)

        result["records"] = records

        log.info(
            log_keyword,
            "Successfully built multiple tracking records from query windows list.",
            TOTAL_RECORDS=len(records),
        )

        return result

    except Exception as e:
        error_message = (
            f"[{log_keyword}] Unexpected error while building multiple tracking records: {e}"
        )
        result["continue_dag_run"] = False
        result["error_message"] = error_message

        log.error(
            log_keyword,
            "Unexpected exception in building_multiple_records_for_tracking_table_from_query_windows_list.",
            exception=str(e),
            error_message=error_message,
        )

        return result



def fill_gap_and_insert_pending_records_for_tracking_table(
    config: Dict[str, Any],
    logger: CustomChainLogger,
    query_tag: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Orchestrator to:
      1) Compute current time context (candidate_start).
      2) Fetch last recorded QUERY_WINDOW_END_TIMESTAMP for this pipeline/config
         from the tracking table (across ALL PIPELINE_STATUS values).
      3) Derive the gap [gap_start, gap_end) to fill:
            gap_start = last_recorded_end
            gap_end   = candidate_start
      4) Generate query-window pairs for the gap.
      5) Build PENDING tracking records for each query window.
      6) Insert those records into the Snowflake tracking table.

    This function ONLY handles the "gap fill" scenario between the
    last recorded window and the current time context. It does not
    handle initial bootstrapping when there are no rows at all.

    Inputs:
      - config : Dict[str, Any]
      - logger : CustomChainLogger
      - query_tag : Optional[str]

    Returns:
      {
          "continue_dag_run": bool,
          "error_message": Optional[str],

          "candidate_start": Optional[datetime],
          "last_recorded_end": Optional[datetime],
          "gap_start": Optional[datetime],
          "gap_end": Optional[datetime],

          "list_of_query_windows": List[Dict[str, Any]],
          "records_to_insert_count": int,
          "inserted_row_count": int,
          "query_id": Optional[str],
      }
    """
    log_keyword = "FILL_GAP_AND_INSERT_PENDING_RECORDS_FOR_TRACKING_TABLE"
    log = logger.new_frame(log_keyword)

    result: Dict[str, Any] = {
        "continue_dag_run": True,
        "error_message": None,

        "candidate_start": None,
        "last_recorded_end": None,
        "gap_start": None,
        "gap_end": None,

        "list_of_query_windows": [],
        "records_to_insert_count": 0,
        "inserted_row_count": 0,
        "query_id": None,
    }

    try:
        log.info(
            log_keyword,
            "Starting gap-fill orchestration for tracking table.",
        )

        # ------------------------------------------------------------------
        # 1) Compute time context → candidate_start
        # ------------------------------------------------------------------
        time_ctx_res = compute_time_context(config=config, logger=log)
        if not time_ctx_res.get("continue_dag_run", False):
            error_message = time_ctx_res.get(
                "error_message",
                f"[{log_keyword}] compute_time_context failed.",
            )
            result["continue_dag_run"] = False
            result["error_message"] = error_message

            log.error(
                log_keyword,
                "_compute_time_context returned continue_dag_run=False.",
                error_message=error_message,
            )
            return result

        candidate_start: Optional[datetime] = time_ctx_res.get("candidate_start")
        result["candidate_start"] = candidate_start

        if candidate_start is None:
            error_message = (
                f"[{log_keyword}] _compute_time_context returned candidate_start=None."
            )
            result["continue_dag_run"] = False
            result["error_message"] = error_message

            log.error(
                log_keyword,
                "candidate_start is None; cannot compute gap.",
            )
            return result

        log.info(
            log_keyword,
            "Time context computed.",
            CANDIDATE_START=str(candidate_start),
        )

        # ------------------------------------------------------------------
        # 2) Fetch last recorded window end (ANY status)
        # ------------------------------------------------------------------
        last_rec_res = fetch_last_recorded_window_end(
            config=config,
            logger=log,
            query_tag=query_tag,
        )

        if not last_rec_res.get("continue_dag_run", False):
            error_message = last_rec_res.get(
                "error_message",
                f"[{log_keyword}] fetch_last_recorded_window_end failed.",
            )
            result["continue_dag_run"] = False
            result["error_message"] = error_message

            log.error(
                log_keyword,
                "fetch_last_recorded_window_end returned continue_dag_run=False.",
                error_message=error_message,
            )
            return result

        last_recorded_end: Optional[datetime] = last_rec_res.get("last_recorded_end")
        result["last_recorded_end"] = last_recorded_end

        if last_recorded_end is None:
            # No rows at all yet for this pipeline + config. This orchestrator
            # is only for gap-fill AFTER something already exists.
            log.info(
                log_keyword,
                "No last_recorded_end found (no existing rows for this pipeline/config). "
                "Gap-based creation is skipped in this function.",
            )
            return result  # continue_dag_run=True, nothing inserted

        log.info(
            log_keyword,
            "Last recorded window end fetched (any status).",
            LAST_RECORDED_END=str(last_recorded_end),
        )

        # ------------------------------------------------------------------
        # 3) Derive gap [gap_start, gap_end)
        # ------------------------------------------------------------------
        gap_start = last_recorded_end
        gap_end = candidate_start

        result["gap_start"] = gap_start
        result["gap_end"] = gap_end

        if gap_start >= gap_end:
            log.info(
                log_keyword,
                "No positive-width gap detected between last_recorded_end and candidate_start.",
                GAP_START=str(gap_start),
                GAP_END=str(gap_end),
            )
            return result  # continue_dag_run=True, nothing inserted

        log.info(
            log_keyword,
            "Positive gap detected; proceeding to create query windows.",
            GAP_START=str(gap_start),
            GAP_END=str(gap_end),
        )

        # ------------------------------------------------------------------
        # 4) Generate query-window pairs for the gap
        # ------------------------------------------------------------------
        windows_res = get_query_window_pairs_list(
            start_query_window_start_timestamp=gap_start,
            end_query_window_start_timestamp=gap_end,
            config=config,
            logger=log,
        )

        if not windows_res.get("continue_dag_run", False):
            error_message = windows_res.get(
                "error_message",
                f"[{log_keyword}] get_query_window_pairs_list failed.",
            )
            result["continue_dag_run"] = False
            result["error_message"] = error_message

            log.error(
                log_keyword,
                "get_query_window_pairs_list returned continue_dag_run=False.",
                error_message=error_message,
            )
            return result

        list_of_query_windows = windows_res.get("list_of_query_windows", []) or []
        result["list_of_query_windows"] = list_of_query_windows

        if not list_of_query_windows:
            log.info(
                log_keyword,
                "No query windows generated for the computed gap. Nothing to insert.",
                GAP_START=str(gap_start),
                GAP_END=str(gap_end),
            )
            return result  # continue_dag_run=True, nothing inserted

        log.info(
            log_keyword,
            "Query window pairs generated for gap.",
            TOTAL_QUERY_WINDOWS=len(list_of_query_windows),
        )

        # ------------------------------------------------------------------
        # 5) Build PENDING tracking records for each query window
        # ------------------------------------------------------------------
        records_build_res = building_multiple_records_for_tracking_table_from_query_windows_list(
            list_of_query_windows=list_of_query_windows,
            config=config,
            logger=log,
        )

        if not records_build_res.get("continue_dag_run", False):
            error_message = records_build_res.get(
                "error_message",
                f"[{log_keyword}] building_multiple_records_for_tracking_table_from_query_windows_list failed.",
            )
            result["continue_dag_run"] = False
            result["error_message"] = error_message

            log.error(
                log_keyword,
                "building_multiple_records_for_tracking_table_from_query_windows_list "
                "returned continue_dag_run=False.",
                error_message=error_message,
            )
            return result

        records_to_insert: List[Dict[str, Any]] = records_build_res.get("records", []) or []
        records_to_insert_count = len(records_to_insert)
        result["records_to_insert_count"] = records_to_insert_count

        if records_to_insert_count == 0:
            log.info(
                log_keyword,
                "No records built from query windows. Nothing to insert.",
            )
            return result  # continue_dag_run=True, nothing inserted

        log.info(
            log_keyword,
            "Built tracking records for gap windows.",
            RECORDS_TO_INSERT_COUNT=records_to_insert_count,
        )

        # ------------------------------------------------------------------
        # 6) Insert records into tracking table
        # ------------------------------------------------------------------
        insert_res = insert_multiple_records_into_tracking_table(
            records=records_to_insert,
            config=config,
            logger=log,
            query_tag=query_tag,
        )

        if not insert_res.get("continue_dag_run", False):
            error_message = insert_res.get(
                "error_message",
                f"[{log_keyword}] insert_multiple_records_into_tracking_table failed.",
            )
            result["continue_dag_run"] = False
            result["error_message"] = error_message
            result["inserted_row_count"] = insert_res.get("inserted_row_count", 0)
            result["query_id"] = insert_res.get("query_id")

            log.error(
                log_keyword,
                "Bulk insert into tracking table failed.",
                error_message=error_message,
                INSERTED_ROW_COUNT=result["inserted_row_count"],
                QUERY_ID=result["query_id"],
            )
            return result

        result["inserted_row_count"] = insert_res.get("inserted_row_count", 0)
        result["query_id"] = insert_res.get("query_id")

        log.info(
            log_keyword,
            "Gap-fill and insert completed successfully.",
            CANDIDATE_START=str(candidate_start),
            LAST_RECORDED_END=str(last_recorded_end),
            GAP_START=str(gap_start),
            GAP_END=str(gap_end),
            TOTAL_QUERY_WINDOWS=len(list_of_query_windows),
            RECORDS_TO_INSERT_COUNT=records_to_insert_count,
            INSERTED_ROW_COUNT=result["inserted_row_count"],
            QUERY_ID=result["query_id"],
        )

        return result

    except Exception as e:
        import traceback

        tb = traceback.format_exc()
        error_message = (
            f"[{log_keyword}] Unexpected error while filling gap and inserting pending records: {e}"
        )
        result["continue_dag_run"] = False
        result["error_message"] = f"{error_message}\n{tb}"

        log.error(
            log_keyword,
            "Unexpected exception in fill_gap_and_insert_pending_records_for_tracking_table.",
            error_message=error_message,
        )
        return result
















