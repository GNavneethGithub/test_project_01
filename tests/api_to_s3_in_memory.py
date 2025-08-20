from typing import Any, Dict, Iterable, List, Optional, Tuple
from datetime import datetime
import io, os, time, json, requests, boto3









def get_total_record_count(config: Dict[str, Any], record: Dict[str, Any]) -> int:
    """
    Return the total number of matching records.

    Tries in order:
      1) Header probe: GET with limit=1 and read total from a response header (default 'X-Total-Count').
      2) Optional count endpoint: if config['count_endpoint'] is provided, call it and parse common fields.
      3) Fallback pagination: page through results with minimal fields and count rows.

    Required in config: api_url, access_token
    Required in record: start_time, end_time
    Optional in config:
      - page_size (default 1000)
      - timeout (default 30)
      - request_backoff_base (default 1.0)
      - request_max_retries (default 5)
      - extra_query_params (dict)
      - timezone (IANA tz name)
      - total_count_header (default "X-Total-Count")
      - count_endpoint (URL to hit for count, same query semantics as api_url)
      - count_fields (default "sys_id")  # used only by pagination fallback
      - debug (default True)
    """
    debug = bool(config.get("debug", True))
    api_url = config["api_url"]
    token = config["access_token"]
    tz_name = config.get("timezone")
    page_size = int(config.get("page_size", 1000))
    timeout = int(config.get("timeout", 30))
    backoff_base = float(config.get("request_backoff_base", 1.0))
    max_retries = int(config.get("request_max_retries", 5))
    extra_params = config.get("extra_query_params") or {}
    header_name = config.get("total_count_header", "X-Total-Count")
    count_endpoint = config.get("count_endpoint")  # optional

    # Use your existing helpers:
    start_s = normalize_time(record["start_time"], tz_name)
    end_s   = normalize_time(record["end_time"], tz_name)
    headers = build_headers(token)
    base_params = build_params(start_s, end_s, page_size, extra_params)

    # ---- 1) Header probe (limit=1) ----
    probe = dict(base_params)
    probe["sysparm_limit"] = 1
    with requests.Session() as session:
        try:
            resp = request_with_retry(
                session, "GET", api_url,
                timeout=timeout, backoff_base=backoff_base, max_retries=max_retries, debug=debug,
                headers=headers, params=probe,
            )
            resp.raise_for_status()
            if header_name in resp.headers:
                try:
                    total = int(resp.headers[header_name])
                    log(debug, f"Count (via header {header_name}): {total}")
                    return total
                except ValueError:
                    log(debug, f"Header {header_name} not integer: {resp.headers[header_name]}")
        except Exception as e:
            log(debug, f"Header probe failed: {e}")

        # ---- 2) Optional count endpoint ----
        if count_endpoint:
            try:
                # Reuse same query; some APIs support flags like sysparm_count=true
                count_params = dict(base_params)
                count_params["sysparm_limit"] = 1
                count_params.setdefault("sysparm_fields", "sys_id")
                # Uncomment if your backend supports it:
                # count_params["sysparm_count"] = "true"

                r = request_with_retry(
                    session, "GET", count_endpoint,
                    timeout=timeout, backoff_base=backoff_base, max_retries=max_retries, debug=debug,
                    headers=headers, params=count_params,
                )
                r.raise_for_status()
                j = r.json()
                for k in ("count", "total", "total_count", "result_count"):
                    if isinstance(j, dict) and k in j:
                        try:
                            total = int(j[k])
                            log(debug, f"Count (via endpoint {k}): {total}")
                            return total
                        except ValueError:
                            pass
                log(debug, "Count endpoint returned no usable total; falling back to pagination.")
            except Exception as e:
                log(debug, f"Count endpoint failed: {e}; falling back to pagination.")

        # ---- 3) Fallback pagination count (minimal fields) ----
        params = dict(base_params)
        params["sysparm_fields"] = config.get("count_fields", "sys_id")
        count = 0
        while True:
            resp = request_with_retry(
                session, "GET", api_url,
                timeout=timeout, backoff_base=backoff_base, max_retries=max_retries, debug=debug,
                headers=headers, params=params,
            )
            resp.raise_for_status()
            data = resp.json()
            batch = data.get("result") or []
            n = len(batch)
            count += n
            log(debug, f"Counted {n} (running total {count}) offset={params['sysparm_offset']}")
            if n == 0:
                break
            params["sysparm_offset"] = int(params.get("sysparm_offset", 0)) + int(params["sysparm_limit"])

        log(debug, f"Count (via pagination): {count}")
        return count




# ---------- small helpers ----------
def log(debug: bool, *a: Any) -> None:
    if debug:
        print(*a)

def dumps_line(obj: Any) -> bytes:
    # NDJSON line (UTF-8, newline-terminated)
    return (json.dumps(obj, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")

def parse_s3_uri(uri: str) -> Tuple[str, str, str]:
    """
    s3://bucket/path/to/name.json -> (bucket, 'path/to', 'name.json')
    """
    if not uri.startswith("s3://"):
        raise ValueError(f"Invalid S3 URI (must start with s3://): {uri}")
    without = uri[5:]
    parts = without.split("/", 1)
    if len(parts) == 1:
        raise ValueError("S3 URI must include a key/path")
    bucket, key = parts[0], parts[1].strip("/")
    if not key:
        raise ValueError("S3 URI must include a key/path after the bucket")
    if "/" in key:
        prefix, fname = key.rsplit("/", 1)
    else:
        prefix, fname = "", key
    return bucket, prefix, fname

def build_s3_client(config: Dict[str, Any]):
    # username/password (access/secret); supports S3-compatible endpoints
    return boto3.client(
        "s3",
        aws_access_key_id=config["s3_username"],
        aws_secret_access_key=config["s3_password"],
        region_name=config.get("s3_region"),
        endpoint_url=config.get("s3_endpoint_url"),
    )

# ---------- API paging (simple, with retry) ----------
def fetch_batches(
    api_url: str,
    access_token: str,
    start_time: str,
    end_time: str,
    page_size: int,
    timeout: int,
    debug: bool,
    extra_params: Optional[Dict[str, Any]] = None,
) -> Iterable[List[Dict[str, Any]]]]:
    """
    Yields batches (<= page_size) as list[dict].
    Retries on 429/5xx (up to 5 attempts with exponential backoff).
    Stops on empty page.
    """
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {access_token}",
    }
    params = {
        "sysparm_query": f"sys_updated_onBETWEEN{start_time}@{end_time}",
        "sysparm_limit": page_size,
        "sysparm_offset": 0,
        "sysparm_display_value": "true",
    }
    if extra_params:
        params.update(extra_params)

    with requests.Session() as session:
        while True:
            attempt, backoff = 0, 1.0
            while True:
                attempt += 1
                try:
                    resp = session.get(api_url, headers=headers, params=params, timeout=timeout)
                    if resp.status_code in (429, 500, 502, 503, 504):
                        raise requests.HTTPError(f"retryable {resp.status_code}", response=resp)
                    resp.raise_for_status()
                    break
                except Exception as e:
                    if attempt >= 5:
                        raise
                    log(debug, f"[retry {attempt}/4] {e}; sleeping {backoff:.1f}s")
                    time.sleep(backoff)
                    backoff *= 2

            data = resp.json()
            batch = data.get("result") or []
            log(debug, f"Fetched batch size: {len(batch)} offset={params['sysparm_offset']}")
            if not batch:
                return
            yield batch
            params["sysparm_offset"] = int(params["sysparm_offset"]) + int(params["sysparm_limit"])

# ---------- In-RAM part builder + uploader ----------
def run_export_in_memory(config: Dict[str, Any], record: Dict[str, Any]) -> List[str]:
    """
    No local storage:
      1) Stream API batches into an in-memory buffer (BytesIO) capped at ≤ part_max_bytes.
         Each buffer is NDJSON (.json extension for object key).
      2) When a buffer reaches the cap, upload it immediately to S3 as one object.
      3) If ANY error occurs, delete all objects uploaded in this run (rollback).
    Returns: list of S3 keys uploaded on success.
    """
    debug = bool(config.get("debug", True))

    # --- required inputs ---
    needed = ["api_url", "access_token", "s3_username", "s3_password"]
    miss = [k for k in needed if not config.get(k)]
    if miss:
        raise ValueError(f"Missing config keys: {miss}")
    if "s3_uri" not in record:
        raise ValueError("record must include 's3_uri' like s3://bucket/path/file.json")
    if "start_time" not in record or "end_time" not in record:
        raise ValueError("record must include 'start_time' and 'end_time' (strings for API filter)")

    api_url     = config["api_url"]
    token       = config["access_token"]
    start_time  = record["start_time"]   # strings expected (already in API format)
    end_time    = record["end_time"]
    page_size   = int(config.get("page_size", 1000))
    timeout     = int(config.get("timeout", 30))
    extra       = config.get("extra_query_params")
    part_max    = int(config.get("part_max_bytes", 250 * 1024 * 1024))  # 250 MB
    index_name  = config.get("index_name", "dataset")

    bucket, prefix, _orig_fname = parse_s3_uri(record["s3_uri"])
    prefix = (prefix or "").strip("/")
    epoch  = str(int(time.time()))
    base   = f"{index_name}_{epoch}"  # final keys: {base}_part-00001.json

    s3 = build_s3_client(config)

    uploaded_keys: List[str] = []
    buf = io.BytesIO()
    buf_size = 0
    part_idx = 0

    def new_key() -> str:
        nonlocal part_idx
        part_idx += 1
        fname = f"{base}_part-{part_idx:05d}.json"
        return f"{prefix}/{fname}" if prefix else fname

    def upload_current_buffer():
        # uploads buf (positioned at 0) to a new key, records key for rollback
        key = new_key()
        log(debug, f"Uploading in-memory part -> s3://{bucket}/{key} ({buf_size} bytes)")
        buf.seek(0)
        s3.upload_fileobj(buf, bucket, key)  # boto3 handles multipart internally
        uploaded_keys.append(key)
        log(debug, f"Uploaded: s3://{bucket}/{key}")

    try:
        # verify bucket is reachable upfront
        s3.head_bucket(Bucket=bucket)

        for batch in fetch_batches(api_url, token, start_time, end_time,
                                   page_size=page_size, timeout=timeout, debug=debug,
                                   extra_params=extra):
            for rec in batch:
                line = dumps_line(rec)
                # rotate if adding this line would exceed the cap and we already have content
                if buf_size > 0 and (buf_size + len(line) > part_max):
                    # finalize & upload current buffer
                    upload_current_buffer()
                    # start a fresh buffer
                    buf = io.BytesIO()
                    buf_size = 0
                # if a single line itself exceeds cap, still upload it alone
                if buf_size == 0 and len(line) > part_max:
                    buf.write(line)
                    buf_size += len(line)
                    upload_current_buffer()
                    buf = io.BytesIO()
                    buf_size = 0
                    continue
                # normal append
                buf.write(line)
                buf_size += len(line)

        # last buffer
        if buf_size > 0:
            upload_current_buffer()

        return uploaded_keys

    except Exception as e:
        log(debug, f"Failure during export: {e}. Rolling back {len(uploaded_keys)} uploaded object(s).")
        # rollback: delete any objects we uploaded in this run
        for key in uploaded_keys:
            try:
                s3.delete_object(Bucket=bucket, Key=key)
                log(debug, f"Rolled back s3://{bucket}/{key}")
            except Exception as de:
                log(debug, f"Rollback failed for {key}: {de}")
        raise
