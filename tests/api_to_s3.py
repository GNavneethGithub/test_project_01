from typing import Any, Dict, Iterable, List, Optional, Tuple
from datetime import datetime
import os, time, json, requests, boto3

# ----------------- simple utils -----------------
def log(debug: bool, *a: Any) -> None:
    if debug:
        print(*a)

def dumps_line(obj: Any) -> bytes:
    # NDJSON: one JSON object per line
    return (json.dumps(obj, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")

def parse_s3_uri(uri: str) -> Tuple[str, str, str]:
    """
    s3://bucket/path/to/name.json  ->  (bucket='bucket', prefix='path/to', filename='name.json')
    prefix can be '' if no path. Raises ValueError on bad URI.
    """
    if not uri.startswith("s3://"):
        raise ValueError(f"Invalid S3 URI (must start with s3://): {uri}")
    without = uri[5:]
    parts = without.split("/", 1)
    if len(parts) == 1:
        raise ValueError("S3 URI must include a key/path")
    bucket, key = parts[0], parts[1]
    key = key.strip("/")
    if not key:
        raise ValueError("S3 URI must include a key/path after the bucket")
    # split off final filename
    if "/" in key:
        prefix, fname = key.rsplit("/", 1)
    else:
        prefix, fname = "", key
    return bucket, prefix, fname

def build_s3_client(config: Dict[str, Any]):
    # username/password style creds
    return boto3.client(
        "s3",
        aws_access_key_id=config["s3_username"],
        aws_secret_access_key=config["s3_password"],
        region_name=config.get("s3_region"),
        endpoint_url=config.get("s3_endpoint_url"),
    )

# ----------------- API paging (simple, robust) -----------------
def fetch_batches(
    api_url: str,
    access_token: str,
    start_time: str,
    end_time: str,
    page_size: int,
    timeout: int,
    debug: bool,
    extra_params: Optional[Dict[str, Any]] = None,
) -> Iterable[List[Dict[str, Any]]]:
    """
    Yields batches (<=page_size) as list[dict].
    Simple retry on 429/5xx; stops on empty page.
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
            # basic retries
            attempt = 0
            backoff = 1.0
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

# ----------------- part writer (rotates at ≤ 250MB) -----------------
def write_parts_to_temp(
    api_url: str,
    access_token: str,
    start_time: str,
    end_time: str,
    temp_dir: str,
    index_name: str,
    *,
    page_size: int = 1000,
    part_max_bytes: int = 250 * 1024 * 1024,
    timeout: int = 30,
    debug: bool = True,
    extra_params: Optional[Dict[str, Any]] = None,
) -> List[str]:
    """
    Streams API data directly into NDJSON part files (≤ part_max_bytes), no intermediate page files.
    Returns list of local part file paths.
    """
    os.makedirs(temp_dir, exist_ok=True)
    epoch = str(int(time.time()))
    base = f"{index_name}_{epoch}"

    part_paths: List[str] = []
    part_idx = 0
    current_fp = None
    current_path = None
    current_size = 0

    def open_new_part():
        nonlocal part_idx, current_fp, current_path, current_size
        part_idx += 1
        current_size = 0
        current_path = os.path.join(temp_dir, f"{base}_part-{part_idx:05d}.json")
        current_fp = open(current_path, "wb")
        log(debug, f"Opened new part: {current_path}")

    def close_part():
        nonlocal current_fp, current_path
        if current_fp and not current_fp.closed:
            current_fp.flush()
            current_fp.close()
            part_paths.append(current_path)
            log(debug, f"Closed part: {current_path} ({os.path.getsize(current_path)} bytes)")
        current_fp = None
        current_path = None

    try:
        for batch in fetch_batches(
            api_url, access_token, start_time, end_time, page_size, timeout, debug, extra_params
        ):
            # encode once per batch
            encoded = [dumps_line(r) for r in batch]

            for line in encoded:
                if current_fp is None:
                    open_new_part()
                if current_size > 0 and (current_size + len(line) > part_max_bytes):
                    close_part()
                    open_new_part()
                current_fp.write(line)
                current_size += len(line)

        # finalize last part if any data was written
        if current_fp is not None:
            close_part()

        if not part_paths:
            log(debug, "No data collected; produced zero part files.")

        return part_paths

    except Exception:
        # on failure during collection: close and remove any partial current part
        try:
            if current_fp and not current_fp.closed:
                current_fp.close()
            if current_path and os.path.exists(current_path):
                os.remove(current_path)
        finally:
            # clean any completed parts too; this run is invalid
            for p in part_paths:
                try: os.remove(p)
                except OSError: pass
        raise

# ----------------- S3 upload with rollback on failure -----------------
def upload_parts_with_rollback(
    s3,
    part_paths: List[str],
    bucket: str,
    prefix: str,
    debug: bool = True,
) -> List[str]:
    """
    Uploads each local part to s3://bucket/prefix/<filename>.json sequentially.
    If any upload fails, deletes all objects uploaded in this run (rollback) and re-raises.
    Returns list of successfully uploaded keys (on success).
    """
    uploaded: List[str] = []
    prefix = (prefix or "").strip("/")
    try:
        # ensure bucket is accessible
        s3.head_bucket(Bucket=bucket)

        for i, path in enumerate(part_paths, 1):
            if not os.path.exists(path):
                log(debug, f"Skip missing file: {path}")
                continue
            fname = os.path.basename(path)
            key = f"{prefix}/{fname}" if prefix else fname
            log(debug, f"Uploading [{i}/{len(part_paths)}] {path} -> s3://{bucket}/{key}")
            s3.upload_file(path, bucket, key)  # defaults are fine
            uploaded.append(key)
            log(debug, f"Uploaded: s3://{bucket}/{key}")

        return uploaded

    except Exception as e:
        log(debug, f"Upload failed: {e}; rolling back {len(uploaded)} object(s).")
        for key in uploaded:
            try:
                s3.delete_object(Bucket=bucket, Key=key)
                log(debug, f"Rolled back s3://{bucket}/{key}")
            except Exception as de:
                log(debug, f"Rollback delete failed for {key}: {de}")
        raise

# ----------------- Orchestrator -----------------
def run_export(config: Dict[str, Any], record: Dict[str, Any]) -> List[str]:
    """
    1) Collect from API -> write NDJSON .json parts (≤250MB) into temp dir
    2) Upload parts to S3 (derived from record['s3_uri'] by dropping the filename and appending our own)
    Always cleans local temp files. Rolls back S3 objects on upload failure.
    """
    debug = bool(config.get("debug", True))

    # required inputs
    required = ["api_url", "access_token", "s3_username", "s3_password"]
    missing = [k for k in required if not config.get(k)]
    if missing:
        raise ValueError(f"Missing config keys: {missing}")
    if "s3_uri" not in record:
        raise ValueError("record must include 's3_uri' like s3://bucket/path/file.json")

    # derive bucket/prefix from record['s3_uri']
    bucket, prefix, _orig_name = parse_s3_uri(record["s3_uri"])

    # choose temp dir and naming
    temp_dir = (config.get("temp_dir") or config.get("local_dir") or "./tmp").rstrip("/\\")
    index_name = config.get("index_name", "dataset")

    # timestamps (treat as strings; caller supplies format used by API)
    start_time = record["start_time"]
    end_time   = record["end_time"]
    if not isinstance(start_time, str) or not isinstance(end_time, str):
        raise ValueError("record['start_time'] and ['end_time'] must be strings formatted for the API filter")

    # collection params
    page_size = int(config.get("page_size", 1000))
    part_max  = int(config.get("part_max_bytes", 250 * 1024 * 1024))
    timeout   = int(config.get("timeout", 30))
    extra_params = config.get("extra_query_params")

    # 1) collect to temp
    part_paths = write_parts_to_temp(
        api_url=config["api_url"],
        access_token=config["access_token"],
        start_time=start_time,
        end_time=end_time,
        temp_dir=temp_dir,
        index_name=index_name,  # base + epoch is done inside
        page_size=page_size,
        part_max_bytes=part_max,
        timeout=timeout,
        debug=debug,
        extra_params=extra_params,
    )

    if not part_paths:
        log(debug, "No parts produced; nothing to upload.")
        return []

    # 2) upload to S3 (rollback on failure)
    s3 = build_s3_client(config)
    try:
        keys = upload_parts_with_rollback(s3, part_paths, bucket, prefix, debug=debug)
        return keys
    finally:
        # always remove local files
        for p in part_paths:
            try:
                os.remove(p)
                log(debug, f"Deleted temp file: {p}")
            except OSError as e:
                log(debug, f"Failed to delete temp file {p}: {e}")
