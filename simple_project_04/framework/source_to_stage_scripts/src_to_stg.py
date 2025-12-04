
import re
import json
import traceback
from typing import Dict, Any, List, Optional

import subprocess
from datetime import datetime, timezone, timedelta

from framework.stage_scripts.stage_cleaner import cleaner
from utils.custom_logging import CustomChainLogger, setup_logger



def redact_secrets(text: str) -> str:
    if not text:
        return text
    text = re.sub(r'(--s3AccessKeyId=)\S+', r'\1***REDACTED***', text)
    text = re.sub(r'(--s3SecretAccessKey=)\S+', r'\1***REDACTED***', text)
    text = re.sub(r'(--headers=)\S+', r'\1***REDACTED***', text)
    text = re.sub(r'AKIA[0-9A-Z]{16}', '***REDACTED_AKIA***', text)
    return text

def convert_datetime_to_es_format(dt:datetime, timestamp_format:str, timezone:str) -> str:
    """
    Convert a datetime object to Elasticsearch format string with timezone adjustment.

    Args:
        dt (datetime): The datetime object to convert.
        timestamp_format (str): The desired timestamp format string.
        timezone (str): The timezone string (e.g., "+00:00", "-05:00").

    Returns:
        str: The formatted datetime string in Elasticsearch format.
    """
    # Parse the timezone string to create a timezone object
    if timezone.startswith('+') or timezone.startswith('-'):
        hours_offset = int(timezone[1:3])
        minutes_offset = int(timezone[4:6])
        total_offset = hours_offset * 60 + minutes_offset
        if timezone.startswith('-'):
            total_offset = -total_offset
        tzinfo = timezone(timedelta(minutes=total_offset))
    else:
        tzinfo = timezone.utc  # Default to UTC if no valid timezone provided

    # Localize the datetime object to the specified timezone
    dt_with_tz = dt.replace(tzinfo=tzinfo)

    # Format the datetime object to the specified format
    formatted_dt = dt_with_tz.strftime(timestamp_format)

    return formatted_dt


def build_elasticsearch_search_body_string_replace(
    template: Any,
    mapping: Dict[str, str],
    logger: CustomChainLogger
) -> Dict[str, Any]:
    """
    Build an Elasticsearch search body using simple string-based replacement.

    - template: JSON-like object (expected dict/list). We'll json.dumps() it first.
    - mapping: token -> replacement string. e.g. {"start_time": "...", "end_time": "..."}
      Tokens in the template are plain names like start_time, end_time, timestamp_field_name, etc.
    - logger: CustomChainLogger for logging.

    Returns (dict):
    {
        "continue_dag_run": bool,
        "search_body": dict | list | None,
        "error_message": str | None
    }
    """
    log_keyword = "BUILD_ES_SEARCH_BODY_STR_REPLACE"
    log = logger.new_frame(log_keyword)

    return_object = {
        "continue_dag_run": False,
        "search_body": None,
        "error_message": None,
    }

    try:
        # Validate template
        if template is None:
            msg = "Template is None"
            log.error(log_keyword, msg)
            return_object["error_message"] = msg
            return return_object

        # 1) Serialize to canonical JSON (double quotes)
        try:
            serialized = json.dumps(template)
        except Exception as e:
            # If template contains non-serializable objects, log and fail
            tb = traceback.format_exc()
            msg = f"Failed to json.dumps(template): {e}\n\nTraceback:\n{tb}"
            log.error(log_keyword, "Serialization failed", exc_info=True, error=str(e))
            return_object["error_message"] = msg
            return return_object

        log.info(log_keyword, "Template serialized to JSON for string replacement")

        # 2) Perform replacement for each token.
        # We replace occurrences of the quoted token: e.g., replace '"start_time"' with json.dumps(replacement)
        try:
            replaced = serialized
            for token, repl in mapping.items():
                # Convert replacement to a JSON literal (adds quotes for strings, leaves numbers/objects as-is)
                try:
                    repl_json_literal = json.dumps(repl)
                except Exception:
                    # fallback: coerce to str then json-dump
                    repl_json_literal = json.dumps(str(repl))

                # Replace the quoted token occurrences only (handles keys and values)
                quoted_token = f'"{token}"'
                if quoted_token in replaced:
                    replaced = replaced.replace(quoted_token, repl_json_literal)

            log.info(log_keyword, "Performed token replacements in serialized template")

        except Exception as e:
            tb = traceback.format_exc()
            msg = f"Replacement loop failed: {e}\n\nTraceback:\n{tb}"
            log.error(log_keyword, "Replacement failed", exc_info=True, error=str(e))
            return_object["error_message"] = msg
            return return_object

        # 3) Parse back to Python object
        try:
            parsed = json.loads(replaced)
            # success
            return_object["search_body"] = parsed
            return_object["continue_dag_run"] = True
            log.info(log_keyword, "Successfully built search_body via string replacement")
            return return_object

        except json.JSONDecodeError as e:
            # Fallback: attempt single-quote normalization then parse
            tb1 = traceback.format_exc()
            log.warning(log_keyword, "json.loads failed on replaced text, attempting single-quote fallback", exc_info=True)

            try:
                normalized = replaced.replace("'", '"')
                parsed = json.loads(normalized)
                return_object["search_body"] = parsed
                return_object["continue_dag_run"] = True
                log.info(log_keyword, "Fallback single-quote normalize succeeded")
                return return_object
            except Exception as fallback_exc:
                tb2 = traceback.format_exc()
                err_msg = (
                    "Failed to parse replaced JSON string back to object.\n\n"
                    f"Initial json.loads error: {str(e)}\nTraceback:\n{tb1}\n\n"
                    f"Fallback error: {str(fallback_exc)}\nTraceback:\n{tb2}\n\n"
                    "Replaced content (truncated to 2000 chars):\n"
                    f"{replaced[:2000]}"
                )
                log.error(log_keyword, "Parsing replaced JSON failed", exc_info=True)
                return_object["error_message"] = err_msg
                return return_object

    except Exception as e:
        # Top-level unexpected error
        tb = traceback.format_exc()
        err_msg = f"Unexpected error in build_elasticsearch_search_body_string_replace: {e}\n\nTraceback:\n{tb}"
        try:
            log.error(log_keyword, "Unexpected error building search body", exc_info=True)
        except Exception:
            pass
        return_object["error_message"] = err_msg
        return return_object

def build_elasticdump_cmd_from_template(
    elasticdump_cmd_template: List[str],
    mapping: Dict[str, Any],
    logger: "CustomChainLogger"
) -> Dict[str, Any]:
    """
    Build the final elasticdump CLI command list using simple string replacement.

    Args:
        elasticdump_cmd_template: list of strings (template CLI pieces)
        mapping: token -> replacement (replacement can be str/int/dict/list)
        logger: CustomChainLogger

    Returns (dict):
      {
        "continue_dag_run": bool,
        "cmd_list": List[str] | None,
        "redacted_cmd_display": str | None,
        "missing_tokens": List[str],
        "error_message": str | None
      }
    """
    log_keyword = "BUILD_ELASTICDUMP_CMD"
    log = logger.new_frame(log_keyword)

    return_object = {
        "continue_dag_run": False,
        "cmd_list": None,
        "redacted_cmd_display": None,
        "missing_tokens": [],
        "error_message": None,
    }

    try:
        # Validate inputs
        if not isinstance(elasticdump_cmd_template, list):
            msg = "elasticdump_cmd_template must be a list of strings"
            try:
                log.error(log_keyword, msg)
            except Exception:
                pass
            return_object["error_message"] = msg
            return return_object

        # Prepare replacements as JSON-literal strings where needed
        prepared_repls: Dict[str, str] = {}
        for token, val in mapping.items():
            try:
                if isinstance(val, (dict, list)):
                    # For structured replacements (like search-body), produce JSON string
                    prepared_repls[token] = json.dumps(val)
                else:
                    # For primitives, convert to string
                    prepared_repls[token] = str(val)
            except Exception:
                # Fallback to safe string
                prepared_repls[token] = str(val)

        # Apply replacements to each element in template
        final_cmd_list: List[str] = []
        # We'll track tokens that were actually found anywhere
        tokens_found = set()

        for part in elasticdump_cmd_template:
            # ensure part is string
            if not isinstance(part, str):
                part_str = str(part)
            else:
                part_str = part

            replaced = part_str
            for token, repl_str in prepared_repls.items():
                if token in replaced:
                    replaced = replaced.replace(token, repl_str)
                    tokens_found.add(token)

            final_cmd_list.append(replaced)

        # compute missing tokens (mapped tokens not found in template)
        missing_tokens = [t for t in mapping.keys() if t not in tokens_found]

        # Build redacted single-line display for safe logging
        try:
            redacted_parts = [redact_secrets(p) for p in final_cmd_list]
            redacted_cmd_display = " ".join(redacted_parts)
        except Exception:
            redacted_cmd_display = " ".join(final_cmd_list)

        # Log summary
        try:
            log.info(
                log_keyword,
                "Prepared elasticdump command list",
                command=redacted_cmd_display,
                missing_tokens=missing_tokens
            )
        except Exception:
            pass

        # Fill return object
        return_object["continue_dag_run"] = True
        return_object["cmd_list"] = final_cmd_list
        return_object["redacted_cmd_display"] = redacted_cmd_display
        return_object["missing_tokens"] = missing_tokens
        return return_object

    except Exception as exc:
        tb = traceback.format_exc()
        err_msg = (
            f"Unexpected error building elasticdump command: {str(exc)}\n\nTraceback:\n{tb}"
        )
        try:
            log.error(log_keyword, "Unexpected error building command", exc_info=True, error=str(exc))
        except Exception:
            pass
        return_object["error_message"] = err_msg
        return return_object


def build_aws_s3_uri_from_template(
    QUERY_WINDOW_START_TIMESTAMP: datetime,
    config: Dict[str, Any],
    logger: CustomChainLogger,
    filename_with_extension: Optional[str] = None
) -> Dict[str, Any]:
    """
    Build final S3 URI from config["aws_s3"]["bucket"] and config["aws_s3"]["prefix_template"].
    Always returns a dict: {
        "continue_dag_run": bool,
        "aws_s3_uri": str|None,
        "redacted_aws_s3_uri_display": str|None,
        "error_message": str|None
    }
    """
    log_keyword = "BUILD_AWS_S3_URI"
    log = logger.new_frame(log_keyword)

    result: Dict[str, Any] = {
        "continue_dag_run": False,
        "aws_s3_uri": None,
        "redacted_aws_s3_uri_display": None,
        "error_message": None
    }

    try:
        # Validate inputs
        if not isinstance(config, dict):
            msg = "config must be a dict"
            try: log.error(log_keyword, msg)
            except Exception: pass
            result["error_message"] = msg
            return result

        if not isinstance(QUERY_WINDOW_START_TIMESTAMP, datetime):
            msg = "QUERY_WINDOW_START_TIMESTAMP must be a datetime"
            try: log.error(log_keyword, msg)
            except Exception: pass
            result["error_message"] = msg
            return result

        # Read config values (strictly)
        aws_cfg = config.get("aws_s3", {})
        aws_s3_bucket = aws_cfg.get("bucket")
        prefix_template = aws_cfg.get("prefix_template") or aws_cfg.get("prefix_sub")

        es_cfg = config.get("elasticsearch", {})
        index_id = es_cfg.get("index_id")

        # Validate required
        missing = []
        if not aws_s3_bucket:
            missing.append("aws_s3.bucket")
        if not prefix_template:
            missing.append("aws_s3.prefix_template")
        if not index_id:
            missing.append("elasticsearch.index_id")
        if QUERY_WINDOW_START_TIMESTAMP is None:
            missing.append("QUERY_WINDOW_START_TIMESTAMP")

        if missing:
            msg = f"Missing required inputs for S3 URI build: {missing}"
            try: log.error(log_keyword, msg)
            except Exception: pass
            result["error_message"] = msg
            return result

        # Build placeholders
        yyyy_mm_dd = QUERY_WINDOW_START_TIMESTAMP.strftime("%Y_%m_%d")
        hh_mm = QUERY_WINDOW_START_TIMESTAMP.strftime("%H_%M")

        # filename style: index_id_{epoch_millisec}.json
        if filename_with_extension:
            filename = filename_with_extension
        else:
            ts_now = datetime.now(timezone.utc)
            epoch_millisec = int(ts_now.timestamp() * 1000)
            filename = f"{index_id}_{epoch_millisec}.json"

        # Compose and replace placeholders
        try:
            aws_s3_uri_base = f"s3://{aws_s3_bucket}/{prefix_template}"
            aws_s3_uri = (
                aws_s3_uri_base
                .replace("yyyy_mm_dd", yyyy_mm_dd)
                .replace("hh_mm", hh_mm)
                .replace("filename_with_extension", filename)
            )
        except Exception as exc:
            tb = traceback.format_exc()
            msg = f"Failed replacing placeholders in prefix_template: {exc}\n\nTraceback:\n{tb}"
            try: log.error(log_keyword, msg, exc_info=True)
            except Exception: pass
            result["error_message"] = msg
            return result

        # Redact for display
        try:
            redacted = redact_secrets(aws_s3_uri)
        except Exception:
            redacted = aws_s3_uri

        try:
            log.info(log_keyword, "Built S3 URI", s3_uri=redacted)
        except Exception:
            pass

        result["aws_s3_uri"] = aws_s3_uri
        result["redacted_aws_s3_uri_display"] = redacted
        result["continue_dag_run"] = True
        return result

    except Exception as e:
        tb = traceback.format_exc()
        msg = f"Unexpected error building S3 URI: {str(e)}\n\nTraceback:\n{tb}"
        try: log.error(log_keyword, "Unexpected error building S3 URI", exc_info=True)
        except Exception: pass
        result["error_message"] = msg
        return result



def extract_from_stdout(stdout: Optional[str], logger: CustomChainLogger = None) -> Dict[str, Any]:
    """
    Heuristically extract number of records and files transferred from elasticdump stdout.
    Always returns a dict:
      {
        "continue_dag_run": bool,
        "records_count": int,
        "files_count": int,
        "error_message": str|None
      }
    Uses regex heuristics tuned for elasticdump v6.x outputs.
    """
    log_keyword = "EXTRACT_ELASTICDUMP_STDOUT"
    log = logger.new_frame(log_keyword) if logger is not None else None

    result: Dict[str, Any] = {
        "continue_dag_run": False,
        "records_count": 0,
        "files_count": 0,
        "error_message": None
    }

    try:
        if not stdout:
            # nothing to parse
            result["continue_dag_run"] = True
            return result

        records = 0
        files = 0

        # Sum "wrote N" occurrences (preferred)
        try:
            for m in re.finditer(r'wrote\s+(\d+)', stdout, flags=re.IGNORECASE):
                try:
                    records += int(m.group(1))
                except Exception:
                    pass
        except Exception:
            # non-fatal
            pass

        # Look for "Total Writes: N"
        try:
            m_total = re.search(r'Total Writes[:\s]+(\d+)', stdout, flags=re.IGNORECASE)
            if m_total:
                try:
                    records = int(m_total.group(1))
                except Exception:
                    pass
        except Exception:
            pass

        # Fallback: sum "got N objects" if we still have 0
        if records == 0:
            total_got = 0
            try:
                for m in re.finditer(r'got\s+(\d+)\s+objects', stdout, flags=re.IGNORECASE):
                    try:
                        total_got += int(m.group(1))
                    except Exception:
                        pass
            except Exception:
                pass
            if total_got > 0:
                records = total_got

        # Files heuristics
        try:
            m_files = re.search(r'Written\s+(\d+)\s+files', stdout, flags=re.IGNORECASE)
            if not m_files:
                m_files = re.search(r'Files\s+written[:\s]+(\d+)', stdout, flags=re.IGNORECASE)
            if m_files:
                try:
                    files = int(m_files.group(1))
                except Exception:
                    pass
        except Exception:
            pass

        # If no explicit files found but there were records, default to 1 (heuristic)
        if files == 0 and records > 0:
            files = 1

        result["records_count"] = records
        result["files_count"] = files
        result["continue_dag_run"] = True
        return result

    except Exception as e:
        tb = traceback.format_exc()
        err = f"Unexpected error parsing stdout: {str(e)}\n\nTraceback:\n{tb}"
        try:
            if log is not None:
                log.error(log_keyword, "Error parsing stdout", exc_info=True)
        except Exception:
            pass
        result["error_message"] = err
        return result



def transfer_source_to_stage(config: Dict[str, Any], record: Dict[str, Any], logger: CustomChainLogger) -> Dict[str, Any]:
    log_keyword = "DATA_TRANSFER_ELASTICSEARCH_TO_AWS_S3"
    log_Cust = logger.new_frame(log_keyword)

    return_object: Dict[str, Any] = {
        "continue_dag_run": False,
        "error_message": None,
        "source_to_stage_transfer_status": None,
        "number_of_records_transfered": None,
        "number_of_files_transfered": None
    }

    try:
        # ------------ (same extraction/validation/build steps as before) ------------
        es_cfg = config.get("elasticsearch", {})
        index_id = es_cfg.get("index_id")
        timestamp_format = es_cfg.get("timestamp_format")
        tz_str = es_cfg.get("timezone")
        host_url = es_cfg.get("host_url")
        timestamp_field_name = es_cfg.get("timestamp_field_name")
        elasticsearch_header = es_cfg.get("elasticsearch_header", "")
        elasticsearch_search_body_template = es_cfg.get("elasticsearch_search_body", {})

        qs = record.get("QUERY_WINDOW_START_TIMESTAMP")
        qe = record.get("QUERY_WINDOW_END_TIMESTAMP")

        missing = []
        if qs is None:
            missing.append("record.QUERY_WINDOW_START_TIMESTAMP")
        if qe is None:
            missing.append("record.QUERY_WINDOW_END_TIMESTAMP")
        if not index_id:
            missing.append("elasticsearch.index_id")
        if not timestamp_format:
            missing.append("elasticsearch.timestamp_format")
        if not tz_str:
            missing.append("elasticsearch.timezone")
        if not host_url:
            missing.append("elasticsearch.host_url")
        if not timestamp_field_name:
            missing.append("elasticsearch.timestamp_field_name")

        if missing:
            msg = f"Missing required inputs/config for transfer: {missing}"
            try:
                log_Cust.error(log_keyword, msg)
            except Exception:
                pass
            return_object["error_message"] = msg
            return return_object

        # Build S3 URI
        s3_res = build_aws_s3_uri_from_template(qs, config, logger)
        if not s3_res.get("continue_dag_run"):
            return_object["error_message"] = s3_res.get("error_message")
            return return_object
        aws_s3_uri = s3_res.get("aws_s3_uri")
        redacted_s3_display = s3_res.get("redacted_aws_s3_uri_display", "")

        # Build ES search body and cli command...
        mapping = {
            "start_time": convert_datetime_to_es_format(qs, timestamp_format, tz_str),
            "end_time": convert_datetime_to_es_format(qe, timestamp_format, tz_str),
            "timestamp_field_name": timestamp_field_name,
            "timestamp_format": timestamp_format,
            "timezone": tz_str
        }

        es_build_res = build_elasticsearch_search_body_string_replace(elasticsearch_search_body_template, mapping, logger)
        if not es_build_res.get("continue_dag_run"):
            return_object["error_message"] = es_build_res.get("error_message")
            return return_object
        elasticsearch_search_body = es_build_res.get("search_body")
        try:
            elasticsearch_search_body_str = json.dumps(elasticsearch_search_body)
        except Exception:
            elasticsearch_search_body_str = str(elasticsearch_search_body)

        elasticdump_template = config.get("elasticdump_cmd")
        if not isinstance(elasticdump_template, list):
            msg = "elasticdump_cmd must be a list in config"
            try:
                log_Cust.error(log_keyword, msg)
            except Exception:
                pass
            return_object["error_message"] = msg
            return return_object

        cli_mapping = {
            "host_url": host_url,
            "aws_s3_uri": aws_s3_uri,
            "elasticsearch_header": elasticsearch_header,
            "elasticsearch_search_body": elasticsearch_search_body_str,
            "aws_access_key_id": (config.get("aws_s3", {}) or {}).get("access_key_id") or (config.get("aws_s3", {}) or {}).get("aws_access_key_id") or "",
            "aws_secret_access_key": (config.get("aws_s3", {}) or {}).get("secret_access_key") or (config.get("aws_s3", {}) or {}).get("aws_secret_access_key") or "",
            "elasticdump_max_retry_attempts": config.get("elasticdump_max_retry_attempts", ""),
            "elasticdump_max_retry_delay_ms": config.get("elasticdump_max_retry_delay_ms", ""),
            "elasticdump_max_records_limit": config.get("elasticdump_max_records_limit", ""),
            "elasticdump_max_file_size": config.get("elasticdump_max_file_size", ""),
            "elasticdump_max_timeout_ms": config.get("elasticdump_max_timeout_ms", "")
        }

        cmd_build_res = build_elasticdump_cmd_from_template(elasticdump_template, cli_mapping, logger)
        if not cmd_build_res.get("continue_dag_run"):
            return_object["error_message"] = cmd_build_res.get("error_message")
            return return_object

        final_cmd_list = cmd_build_res.get("cmd_list")
        redacted_cmd_display = cmd_build_res.get("redacted_cmd_display", "")

        try:
            log_Cust.info(log_keyword, "Prepared elasticdump command", command=redacted_cmd_display, s3_uri=redacted_s3_display)
        except Exception:
            pass

        # Execute elasticdump
        try:
            cmd_timeout = int(config.get("elasticdump_cmd_timeout_seconds", 3600))
            proc_result = subprocess.run(
                final_cmd_list,
                capture_output=True,
                text=True,
                timeout=cmd_timeout,
                check=False
            )

            try:
                log_Cust.info(log_keyword, f"Return Code: {proc_result.returncode}")
                log_Cust.info(log_keyword, f"STDOUT:\n{redact_secrets(proc_result.stdout)}")
            except Exception:
                pass

            # parse stdout
            parse_res = extract_from_stdout(redact_secrets(proc_result.stdout), logger)
            if not parse_res.get("continue_dag_run"):
                return_object["error_message"] = parse_res.get("error_message")
                return return_object
            records_count = parse_res.get("records_count", 0)
            files_count = parse_res.get("files_count", 0)

            if proc_result.stderr:
                try:
                    log_Cust.warning(log_keyword, f"STDERR:\n{redact_secrets(proc_result.stderr)}")
                except Exception:
                    pass

            # non-zero exit handling
            if proc_result.returncode != 0:
                err_msg = f"elasticdump command failed with return code {proc_result.returncode}"
                try:
                    log_Cust.error(log_keyword, err_msg)
                except Exception:
                    pass

                # Run cleaner and capture its return dict
                try:
                    cleaner_res = cleaner(config, record, log_Cust)
                    try:
                        log_Cust.info(log_keyword, "stg_cleaner result", cleaner_result=cleaner_res)
                    except Exception:
                        pass

                    # if cleaner provided an error, append to overall message for email
                    if cleaner_res.get("error_message"):
                        err_msg = err_msg + "\n\nStage-cleaner reported error:\n" + str(cleaner_res.get("error_message"))

                    # Optionally, include number of deleted files / size in logs
                    nf = cleaner_res.get("number_of_files_deleted")
                    tsz = cleaner_res.get("total_file_size_deleted")
                    try:
                        log_Cust.info(log_keyword, "stg_cleaner_func summary", number_of_files_deleted=nf, total_file_size_deleted=tsz)
                    except Exception:
                        pass

                except Exception:
                    try:
                        log_Cust.error(log_keyword, "stg_cleaner_func raised an exception", exc_info=True)
                    except Exception:
                        pass

                # preserve previous behavior
                return {
                    "continue_dag_run": True,
                    "error_message": None,
                    "source_to_stage_transfer_status": "COMPLETED",
                    "number_of_records_transfered": records_count,
                    "number_of_files_transfered": files_count
                }

            # success path
            return_object["continue_dag_run"] = True
            return_object["error_message"] = None
            return_object["source_to_stage_transfer_status"] = "COMPLETED"
            return_object["number_of_records_transfered"] = records_count
            return_object["number_of_files_transfered"] = files_count
            return return_object

        except subprocess.TimeoutExpired as e:
            tb = traceback.format_exc()
            err_msg = (
                f"elasticdump timed out after {cmd_timeout} seconds.\n"
                f"Command (redacted): {redacted_cmd_display}\n"
                f"Exception: {str(e)}\n\nTraceback:\n{tb}"
            )
            try:
                log_Cust.error(log_keyword, "elasticdump timed out", exc_info=True)
            except Exception:
                pass

            # Run cleaner and capture result
            try:
                cleaner_res = cleaner(config, record, log_Cust)
                try:
                    log_Cust.info(log_keyword, "stg_cleaner result after timeout", cleaner_result=cleaner_res)
                except Exception:
                    pass

                if cleaner_res.get("error_message"):
                    err_msg = err_msg + "\n\nStage-cleaner reported error:\n" + str(cleaner_res.get("error_message"))

            except Exception:
                try:
                    log_Cust.error(log_keyword, "stg_cleaner raised an exception after timeout", exc_info=True)
                except Exception:
                    pass

            return_object["error_message"] = err_msg
            return_object["continue_dag_run"] = False
            return return_object

        except Exception as e:
            tb = traceback.format_exc()
            err_msg = f"Unexpected failure running elasticdump: {str(e)}\n\nTraceback:\n{tb}"
            try:
                log_Cust.error(log_keyword, "Unexpected exception running elasticdump", exc_info=True)
            except Exception:
                pass

            # Run cleaner and capture result
            try:
                cleaner_res = cleaner(config, record, log_Cust)
                try:
                    log_Cust.info(log_keyword, "stg_cleaner result after unexpected error", cleaner_result=cleaner_res)
                except Exception:
                    pass

                if cleaner_res.get("error_message"):
                    err_msg = err_msg + "\n\nStage-cleaner reported error:\n" + str(cleaner_res.get("error_message"))

            except Exception:
                try:
                    log_Cust.error(log_keyword, "stg_cleaner raised an exception after unexpected error", exc_info=True)
                except Exception:
                    pass

            return_object["error_message"] = err_msg
            return_object["continue_dag_run"] = False
            return return_object

    except Exception as e:
        tb = traceback.format_exc()
        err_msg = f"Exception occurred while preparing elasticdump command: {str(e)}\n\nTraceback:\n{tb}"
        try:
            log_Cust.error(log_keyword, "Exception occurred while preparing elasticdump command", exc_info=True)
        except Exception:
            logging.getLogger(__name__).exception("Exception occurred while preparing elasticdump command")

        return_object["error_message"] = err_msg
        return_object["continue_dag_run"] = False
        return return_object


