from typing import Any, Dict, Optional
from datetime import datetime
from zoneinfo import ZoneInfo
import requests
import time

def get_total_record_count(config: Dict[str, Any], record: Dict[str, Any]) -> int:
    """
    Returns total number of records for the given time filter (always an int).
    Requires: config['api_url'], config['access_token'], record['start_time'], record['end_time'].
    """
    # --- validate ---
    api_url = config.get("api_url")
    token   = config.get("access_token")
    if not api_url or not token:
        raise ValueError("config['api_url'] and config['access_token'] are required")
    if "start_time" not in record or "end_time" not in record:
        raise ValueError("record['start_time'] and record['end_time'] are required")

    page_size   = int(config.get("page_size", 1000))
    timeout     = int(config.get("timeout", 30))
    backoff     = float(config.get("request_backoff_base", 1.0))
    max_retries = int(config.get("request_max_retries", 5))
    tz_name: Optional[str] = config.get("timezone")
    debug = bool(config.get("debug", True))

    # --- normalize times (string passthrough; datetime -> 'YYYY-MM-DD HH:MM:SS') ---
    def _norm(val):
        if isinstance(val, str):
            return val
        if not isinstance(val, datetime):
            raise TypeError("start_time/end_time must be str or datetime")
        if tz_name:
            tz = ZoneInfo(tz_name)
            val = val.replace(tzinfo=tz) if val.tzinfo is None else val.astimezone(tz)
        return val.strftime("%Y-%m-%d %H:%M:%S")

    start_s = _norm(record["start_time"])
    end_s   = _norm(record["end_time"])
    if start_s > end_s:
        raise ValueError(f"start_time '{start_s}' is after end_time '{end_s}'")

    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {token}",
    }

    # header-probe params
    probe_params = {
        "sysparm_query": f"sys_updated_onBETWEEN{start_s}@{end_s}",
        "sysparm_limit": 1,
        "sysparm_offset": 0,
        "sysparm_display_value": "true",
    }

    def _get(session: requests.Session, params: Dict[str, Any]):
        attempt, delay = 0, backoff
        while True:
            attempt += 1
            try:
                r = session.get(api_url, headers=headers, params=params, timeout=timeout)
                if r.status_code in (429, 500, 502, 503, 504):
                    raise requests.HTTPError(f"retryable {r.status_code}", response=r)
                r.raise_for_status()
                return r
            except Exception as e:
                if attempt >= max_retries:
                    if debug: print(f"[FINAL RETRY FAILURE] {e}")
                    raise
                if debug: print(f"[retry {attempt}/{max_retries-1}] {e}; sleep {delay:.1f}s")
                time.sleep(delay)
                delay *= 2

    with requests.Session() as session:
        # 1) Try X-Total-Count header (most efficient)
        try:
            r = _get(session, probe_params)
            hdr = r.headers.get("X-Total-Count")
            if hdr is not None:
                total = int(hdr.strip())  # force int
                if debug: print(f"Count via header X-Total-Count: {total}")
                return total
        except Exception as e:
            if debug: print(f"Header probe failed: {e}")

        # 2) Fallback: paginate with minimal fields and count
        params = {
            "sysparm_query": f"sys_updated_onBETWEEN{start_s}@{end_s}",
            "sysparm_limit": page_size,
            "sysparm_offset": 0,
            "sysparm_display_value": "true",
            "sysparm_fields": "sys_id",  # minimal payload
        }

        total = 0
        while True:
            r = _get(session, params)
            data = r.json()
            batch = data.get("result") or []
            n = int(len(batch))  # ensure int
            total += n
            if debug: print(f"counted {n} (running {total}) offset={params['sysparm_offset']}")
            if n == 0:
                break
            params["sysparm_offset"] = int(params["sysparm_offset"]) + int(params["sysparm_limit"])

        return int(total)  # guarantee int on return
