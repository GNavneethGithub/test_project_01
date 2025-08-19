# NDJSON pipeline (files use .json extension)
# - Phase 1: fetch ~page_size rows/page -> write each page to local .json (NDJSON)
# - Phase 2: merge page files into ≤250 MB part .json files (record-safe; no line splits)
# - Phase 3: sequentially upload part files to S3 using username/password credentials
# - Phase 4: delete ALL local files created (pages + parts)
#
# Notes:
# - Content is NDJSON (one JSON object per line), but filenames end with .json as requested.
# - Timezone for timestamps is honored via config['timezone'] (IANA TZ like "Asia/Kolkata").
# - A preflight request probes X-Total-Count if the API provides it.
# - ROBUST: Guaranteed cleanup on any failure, proper resource management, comprehensive error handling.

from __future__ import annotations
from typing import Any, Dict, Iterable, List, Optional, Tuple
from datetime import datetime
from zoneinfo import ZoneInfo
import os
import time
import json
import uuid
import requests
import boto3
from contextlib import contextmanager

# ---------- fast JSON to bytes (NDJSON lines) ----------
try:
    import orjson
    def dumps_bytes(obj: Any) -> bytes:  # includes trailing '\n'
        return orjson.dumps(obj) + b"\n"
except Exception:
    def dumps_bytes(obj: Any) -> bytes:
        return (json.dumps(obj, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")

# ---------- logging ----------
def log(debug: bool, *a: Any) -> None:
    if debug:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{timestamp}]", *a)

# ---------- time handling ----------
def normalize_time(val: Any, tz_name: Optional[str]) -> str:
    """
    Return 'YYYY-MM-DD HH:MM:SS' string. Honors config['timezone'] if provided.
    str -> passthrough; datetime -> interpret/convert with tz if provided.
    """
    if isinstance(val, str):
        return val
    if not isinstance(val, datetime):
        raise TypeError("start_time/end_time must be str or datetime")
    if tz_name:
        tz = ZoneInfo(tz_name)
        if val.tzinfo is None:
            val = val.replace(tzinfo=tz)
        else:
            val = val.astimezone(tz)
    return val.strftime("%Y-%m-%d %H:%M:%S")

# ---------- API request helpers ----------
def build_headers(token: str) -> Dict[str, str]:
    return {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {token}",
    }

def build_params(start_s: str, end_s: str, page_size: int, extra: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    p = {
        "sysparm_query": f"sys_updated_onBETWEEN{start_s}@{end_s}",
        "sysparm_limit": int(page_size),
        "sysparm_offset": 0,
        "sysparm_display_value": "true",
    }
    if extra:
        p.update(extra)
    return p

def request_with_retry(
    session: requests.Session,
    method: str,
    url: str,
    *,
    timeout: int,
    backoff_base: float,
    max_retries: int,
    debug: bool,
    **kwargs: Any,
) -> requests.Response:
    for attempt in range(1, max_retries + 1):
        try:
            resp = session.request(method, url, timeout=timeout, **kwargs)
            if resp.status_code in (429, 500, 502, 503, 504):
                raise requests.HTTPError(f"retryable {resp.status_code}", response=resp)
            return resp
        except Exception as e:
            if attempt == max_retries:
                log(debug, f"[FINAL RETRY FAILURE] {method} {url}: {e}")
                raise
            sleep_s = backoff_base * (2 ** (attempt - 1))
            log(debug, f"[retry {attempt}/{max_retries-1}] error={e} sleep {sleep_s:.1f}s")
            time.sleep(sleep_s)

def preflight_total_count(
    session: requests.Session,
    url: str,
    headers: Dict[str, str],
    params: Dict[str, Any],
    *,
    timeout: int,
    backoff_base: float,
    max_retries: int,
    debug: bool,
) -> Optional[int]:
    """
    Do a lightweight GET (limit=1) and read X-Total-Count if present.
    Returns integer or None.
    """
    probe = dict(params)
    probe["sysparm_limit"] = 1
    try:
        resp = request_with_retry(
            session, "GET", url,
            timeout=timeout, backoff_base=backoff_base, max_retries=max_retries, debug=debug,
            headers=headers, params=probe,
        )
        resp.raise_for_status()
        total = resp.headers.get("X-Total-Count")
        if total is None:
            log(debug, "Preflight: X-Total-Count not present.")
            return None
        try:
            n = int(total)
            log(debug, f"Preflight total count: {n}")
            return n
        except ValueError:
            log(debug, f"Preflight: non-integer X-Total-Count='{total}'")
            return None
    except Exception as e:
        log(debug, f"Preflight request failed: {e}")
        return None

def iter_api_batches(
    session: requests.Session,
    url: str,
    headers: Dict[str, str],
    params: Dict[str, Any],
    *,
    timeout: int,
    backoff_base: float,
    max_retries: int,
    debug: bool,
) -> Iterable[List[Dict[str, Any]]]:
    """
    Yield list[dict] per page (<= page_size). Stops on empty page or when X-Total-Count reached.
    """
    total_records: Optional[int] = None
    seen = 0
    page = 0

    while True:
        page += 1
        log(debug, f"\n=== Request page {page} === offset={params.get('sysparm_offset')} "
                   f"limit={params.get('sysparm_limit')}")
        resp = request_with_retry(
            session, "GET", url,
            timeout=timeout, backoff_base=backoff_base, max_retries=max_retries, debug=debug,
            headers=headers, params=params,
        )
        log(debug, "HTTP", resp.status_code, "URL:", resp.url)

        for k, v in resp.headers.items():
            if k.lower().startswith("x-"):
                log(debug, f"header {k}: {v}")

        resp.raise_for_status()

        if total_records is None and "X-Total-Count" in resp.headers:
            try:
                total_records = int(resp.headers["X-Total-Count"])
                log(debug, "Total records (from page):", total_records)
            except ValueError:
                pass

        data = resp.json()
        batch = data.get("result") or []
        bsz = len(batch)
        log(debug, "batch_size:", bsz)

        if bsz == 0:
            log(debug, "Empty page -> stop.")
            break

        yield batch

        seen += bsz
        if total_records is not None and seen >= total_records:
            log(debug, "Reached X-Total-Count -> stop.")
            break

        params["sysparm_offset"] = int(params.get("sysparm_offset", 0)) + int(params["sysparm_limit"])

# ---------- S3 + naming ----------
def build_s3_client_userpass(config: Dict[str, Any]):
    """
    Build S3 client using username/password style creds.
    Required: config['s3_username'], config['s3_password'], config['s3_bucket']
    Optional: config['s3_region'], config['s3_endpoint_url'] (S3-compatible)
    """
    access_key = config["s3_username"]
    secret_key = config["s3_password"]
    region     = config.get("s3_region")
    endpoint   = config.get("s3_endpoint_url")  # e.g., https://s3.amazonaws.com or MinIO URL
    
    try:
        session = boto3.session.Session(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
        )
        return session.client("s3", endpoint_url=endpoint)
    except Exception as e:
        raise RuntimeError(f"Failed to create S3 client: {e}")

def build_s3_prefix(config: Dict[str, Any], record: Dict[str, Any]) -> str:
    """
    Compose final prefix from config + record.
    Example: config['s3_prefix']='exports/sn', record['s3_subprefix']='incidents/2025-08-19'
    -> 'exports/sn/incidents/2025-08-19'
    """
    parts: List[str] = []
    v = (config.get("s3_prefix") or "").strip("/")
    if v:
        parts.append(v)
    # allow record to add more
    for key in ("s3_subprefix", "prefix"):
        rv = (record.get(key) or "").strip("/")
        if rv:
            parts.append(rv)
    return "/".join(parts)

def build_file_base(index_name: str, start_s: str, end_s: str) -> str:
    """
    Meaningful base: {index_name}_{startYYYYMMDDTHHMMSS}_{endYYYYMMDDTHHMMSS}_{epoch}
    """
    def compact(ts: str) -> str:
        return ts.replace("-", "").replace(":", "").replace(" ", "T")
    epoch = str(int(time.time()))
    return f"{index_name}_{compact(start_s)}_{compact(end_s)}_{epoch}"

# ---------- Safe file operations ----------
@contextmanager
def safe_file_write(filepath: str, debug: bool):
    """Context manager for safe file writing with automatic cleanup on error."""
    fp = None
    try:
        fp = open(filepath, "wb")
        yield fp
        fp.flush()
    except Exception as e:
        log(debug, f"Error writing to {filepath}: {e}")
        if fp and not fp.closed:
            fp.close()
        # Remove partially written file
        try:
            if os.path.exists(filepath):
                os.remove(filepath)
                log(debug, f"Cleaned up partial file: {filepath}")
        except OSError as cleanup_e:
            log(debug, f"Failed to cleanup partial file {filepath}: {cleanup_e}")
        raise
    finally:
        if fp and not fp.closed:
            fp.close()

def ensure_directory(path: str, debug: bool) -> None:
    """Safely create directory with proper error handling."""
    try:
        os.makedirs(path, exist_ok=True)
        log(debug, f"Ensured directory exists: {path}")
    except OSError as e:
        raise RuntimeError(f"Failed to create directory {path}: {e}")

def get_file_size(filepath: str) -> int:
    """Get file size safely, return 0 if file doesn't exist."""
    try:
        return os.path.getsize(filepath)
    except OSError:
        return 0

# ---------- Phase 1: write each page (~1000 rows) to its own NDJSON .json ----------
def write_pages_to_disk(
    session: requests.Session,
    api_url: str,
    headers: Dict[str, str],
    params: Dict[str, Any],
    local_dir: str,
    base: str,
    *,
    timeout: int,
    backoff_base: float,
    max_retries: int,
    debug: bool,
) -> Tuple[List[str], int]:
    ensure_directory(local_dir, debug)
    page_paths: List[str] = []
    total_rows = 0
    page_idx = 0

    try:
        for batch in iter_api_batches(session, api_url, headers, dict(params),
                                      timeout=timeout, backoff_base=backoff_base,
                                      max_retries=max_retries, debug=debug):
            page_idx += 1
            path = os.path.join(local_dir, f"{base}_page-{page_idx:05d}.json")  # NDJSON content, .json extension
            cnt = 0
            
            with safe_file_write(path, debug) as fp:
                for row in batch:   # ~page_size rows
                    fp.write(dumps_bytes(row))
                    cnt += 1
            
            page_paths.append(path)
            total_rows += cnt
            file_size = get_file_size(path)
            log(debug, f"Wrote page {page_idx} -> {path} ({cnt} rows, {file_size} bytes)")
            
    except Exception as e:
        log(debug, f"Phase 1 failed at page {page_idx}: {e}")
        # Cleanup any pages written so far
        cleanup_files(page_paths, debug)
        raise RuntimeError(f"Phase 1 (write pages) failed: {e}")

    return page_paths, total_rows

# ---------- Phase 2: merge page files into ≤250 MB parts (line-safe) ----------
def merge_pages_to_parts(
    page_paths: List[str],
    dest_dir: str,
    base: str,
    part_max_bytes: int,
    debug: bool,
) -> List[str]:
    ensure_directory(dest_dir, debug)
    part_paths: List[str] = []
    part_idx = 0
    current_fp = None
    current_path = None
    current_size = 0

    def open_new_part():
        nonlocal part_idx, current_fp, current_path, current_size
        part_idx += 1
        current_size = 0
        current_path = os.path.join(dest_dir, f"{base}_part-{part_idx:05d}.json")
        current_fp = open(current_path, "wb")
        log(debug, f"Opened part: {current_path}")

    def close_part():
        nonlocal current_fp, current_path
        if current_fp and not current_fp.closed:
            current_fp.flush()
            current_fp.close()
            if current_path:
                part_paths.append(current_path)
                file_size = get_file_size(current_path)
                log(debug, f"Closed part: {current_path} ({file_size} bytes)")
            current_fp = None
            current_path = None

    try:
        for page_path in page_paths:
            if not os.path.exists(page_path):
                log(debug, f"Warning: Page file not found: {page_path}")
                continue
                
            try:
                with open(page_path, "rb") as pf:
                    for line in pf:  # each line is one JSON record
                        if current_fp is None:
                            open_new_part()
                        line_len = len(line)
                        # rotate if adding would exceed limit and current part already has content
                        if current_size > 0 and (current_size + line_len > part_max_bytes):
                            close_part()
                            open_new_part()
                        current_fp.write(line)
                        current_size += line_len
            except OSError as e:
                log(debug, f"Error reading page file {page_path}: {e}")
                raise
                
        close_part()
        
    except Exception as e:
        log(debug, f"Phase 2 failed during merge: {e}")
        # Cleanup: close current file and remove any partial parts
        if current_fp and not current_fp.closed:
            current_fp.close()
        if current_path and os.path.exists(current_path):
            try:
                os.remove(current_path)
                log(debug, f"Cleaned up partial part: {current_path}")
            except OSError:
                pass
        # Cleanup any completed parts
        cleanup_files(part_paths, debug)
        raise RuntimeError(f"Phase 2 (merge pages) failed: {e}")

    if not part_paths:
        log(debug, "Warning: No parts were created (empty dataset)")

    return part_paths

# ---------- Phase 3: upload parts to S3 (sequential) ----------
def upload_parts_to_s3(
    s3,
    part_paths: List[str],
    bucket: str,
    prefix: str,
    debug: bool,
) -> List[str]:
    keys: List[str] = []
    
    # Validate bucket exists
    try:
        s3.head_bucket(Bucket=bucket)
    except Exception as e:
        raise RuntimeError(f"Cannot access S3 bucket '{bucket}': {e}")
    
    for i, p in enumerate(part_paths, 1):
        if not os.path.exists(p):
            log(debug, f"Warning: Part file not found: {p}")
            continue
            
        key = f"{prefix}/{os.path.basename(p)}" if prefix else os.path.basename(p)
        file_size = get_file_size(p)
        
        try:
            log(debug, f"Uploading [{i}/{len(part_paths)}] {p} -> s3://{bucket}/{key} ({file_size} bytes)")
            # Minimal: rely on boto3 defaults; no TransferConfig, no ExtraArgs
            s3.upload_file(p, bucket, key)
            keys.append(key)
            log(debug, f"Successfully uploaded: {key}")
        except Exception as e:
            log(debug, f"Failed to upload {p} to s3://{bucket}/{key}: {e}")
            raise RuntimeError(f"Phase 3 (S3 upload) failed on file {p}: {e}")
    
    return keys

# ---------- Phase 4: cleanup ----------
def cleanup_files(paths: List[str], debug: bool) -> None:
    """Safely remove files with comprehensive error handling."""
    if not paths:
        return
        
    log(debug, f"Cleaning up {len(paths)} files...")
    success_count = 0
    
    for p in paths:
        try:
            if os.path.exists(p):
                os.remove(p)
                log(debug, f"Deleted {p}")
                success_count += 1
            else:
                log(debug, f"File already removed: {p}")
        except OSError as e:
            log(debug, f"Failed to delete {p}: {e}")
    
    log(debug, f"Cleanup complete: {success_count}/{len(paths)} files removed")

# ---------- Orchestrator with guaranteed cleanup ----------
def export_api_to_s3_two_phase(config: Dict[str, Any], record: Dict[str, Any]) -> List[str]:
    """
    Phase 0: preflight count (if header available).
    Phase 1: fetch page_size at a time, write each page to {local_dir}/{base}_page-xxxxx.json (NDJSON)
    Phase 2: merge page files into ≤250MB {base}_part-xxxxx.json (NDJSON, line-safe)
    Phase 3: upload parts sequentially to S3 (keys end with .json)
    Phase 4: delete ALL local files (pages + parts).
    
    ROBUST: Guaranteed cleanup on ANY failure, comprehensive error handling.
    Returns: list of S3 object keys uploaded.
    """
    debug = bool(config.get("debug", True))
    page_paths: List[str] = []
    part_paths: List[str] = []
    
    try:
        # Validate required config
        required_keys = ["api_url", "access_token", "s3_bucket", "s3_username", "s3_password", "local_dir"]
        missing = [key for key in required_keys if not config.get(key)]
        if missing:
            raise ValueError(f"Missing required config keys: {missing}")
        
        api_url = config["api_url"]
        access_token = config["access_token"]
        bucket = config["s3_bucket"]
        local_dir = config["local_dir"]
        tz_name = config.get("timezone")
        page_size = int(config.get("page_size", 1000))
        timeout = int(config.get("timeout", 30))
        backoff_base = float(config.get("request_backoff_base", 1.0))
        max_retries = int(config.get("request_max_retries", 5))
        part_max = int(config.get("part_max_bytes", 250 * 1024 * 1024))  # 250 MB

        # Validate time inputs
        try:
            start_s = normalize_time(record["start_time"], tz_name)
            end_s   = normalize_time(record["end_time"], tz_name)
        except Exception as e:
            raise ValueError(f"Invalid time configuration: {e}")

        # naming and prefix
        index_name = config.get("index_name", "dataset")
        base_name  = build_file_base(index_name, start_s, end_s)  # includes epoch
        prefix     = build_s3_prefix(config, record)

        log(debug, f"=== Starting export: {base_name} ===")
        log(debug, f"Time range: {start_s} to {end_s}")
        log(debug, f"S3 destination: s3://{bucket}/{prefix}")

        headers = build_headers(access_token)
        params  = build_params(start_s, end_s, page_size, config.get("extra_query_params"))

        session = requests.Session()

        # Phase 0: preflight
        _ = preflight_total_count(
            session, api_url, headers, dict(params),
            timeout=timeout, backoff_base=backoff_base, max_retries=max_retries, debug=debug
        )

        # S3 client (username/password)
        s3 = build_s3_client_userpass(config)

        # PHASE 1: Write pages to disk
        log(debug, "\n=== PHASE 1: Writing pages to disk ===")
        page_paths, total_rows = write_pages_to_disk(
            session, api_url, headers, params, local_dir, base_name,
            timeout=timeout, backoff_base=backoff_base, max_retries=max_retries, debug=debug,
        )
        log(debug, f"Phase 1 complete. Pages: {len(page_paths)}; Rows: {total_rows}")

        if not page_paths:
            log(debug, "No data retrieved; nothing to merge/upload.")
            return []

        # PHASE 2: Merge pages into parts
        log(debug, "\n=== PHASE 2: Merging pages into parts ===")
        part_paths = merge_pages_to_parts(page_paths, local_dir, base_name, part_max, debug)
        log(debug, f"Phase 2 complete. Parts: {len(part_paths)}")

        if not part_paths:
            log(debug, "No parts created; cleaning up pages.")
            cleanup_files(page_paths, debug)
            return []

        # PHASE 3: Upload to S3
        log(debug, "\n=== PHASE 3: Uploading parts to S3 ===")
        s3_keys = upload_parts_to_s3(s3, part_paths, bucket, prefix, debug)
        log(debug, f"Phase 3 complete. Uploaded: {s3_keys}")

        # PHASE 4: Cleanup on success
        log(debug, "\n=== PHASE 4: Cleaning up local files ===")
        cleanup_files(page_paths + part_paths, debug)
        log(debug, "Phase 4 complete. Local cleanup done.")

        log(debug, f"=== Export successful: {len(s3_keys)} files uploaded ===")
        return s3_keys

    except Exception as e:
        # CRITICAL: Always cleanup on ANY failure
        log(debug, f"\n=== EXPORT FAILED: {e} ===")
        log(debug, "Performing emergency cleanup...")
        all_files = page_paths + part_paths
        if all_files:
            cleanup_files(all_files, debug)
        else:
            log(debug, "No local files to cleanup.")
        raise





