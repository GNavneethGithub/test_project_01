import requests, json, time, logging, io, boto3
from datetime import datetime, timezone
from dateutil import parser as dtparser  # robust ISO parser

PAGE_SIZE = 200

# ---------------------------
# Logging Setup
# ---------------------------
logging.basicConfig(
    level=logging.DEBUG,  # DEBUG logs will include full API records
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# ---------------------------
# Time Helpers
# ---------------------------
def iso_to_utc(ts: str) -> datetime:
    """Convert ISO8601 string with offset to naive UTC datetime."""
    logging.debug(f"Parsing timestamp: {ts}")
    dt = dtparser.isoparse(ts)            # aware datetime
    logging.debug(f"Parsed datetime (aware): {dt} (tz={dt.tzinfo})")
    dt_utc = dt.astimezone(timezone.utc)  # convert to UTC
    logging.debug(f"Converted to UTC: {dt_utc}")
    return dt_utc.replace(tzinfo=None)    # drop tzinfo for ServiceNow

def sanitize_for_filename(dt: datetime) -> str:
    """Format datetime as safe string for filenames."""
    return dt.strftime("%Y-%m-%d_%H-%M-%S")

# ---------------------------
# ServiceNow Query Builder
# ---------------------------
def build_query(start_dt, end_dt):
    s = start_dt.strftime("%Y-%m-%d %H:%M:%S")
    e = end_dt.strftime("%Y-%m-%d %H:%M:%S")
    query = f"sys_created_on>={s}^sys_created_on<={e}"
    logging.debug(f"Built query: {query}")
    return query

# ---------------------------
# Count Only
# ---------------------------
def get_the_count_from_servicenow_for_table(table, start_ts, end_ts, token, instance, page_size=PAGE_SIZE):
    start = iso_to_utc(start_ts) if isinstance(start_ts, str) else start_ts
    end   = iso_to_utc(end_ts) if isinstance(end_ts, str) else end_ts

    logging.info(f"Counting records for {table} between {start} and {end} (UTC)")

    url = f"https://{instance}.service-now.com/api/now/table/{table}"
    headers = {"Accept": "application/json", "Authorization": f"Bearer {token}"}
    query = build_query(start, end)

    total, offset = 0, 0
    while True:
        params = {
            "sysparm_query": query,
            "sysparm_limit": page_size,
            "sysparm_offset": offset,
            "sysparm_display_value": "true",
            "sysparm_exclude_reference_link": "true"
        }
        logging.debug(f"Request URL: {url} with params={params}")
        r = requests.get(url, headers=headers, params=params, timeout=60)
        logging.debug(f"HTTP {r.status_code}, response length={len(r.text)}")
        if r.status_code != 200:
            logging.error(f"Error {r.status_code}: {r.text[:500]}")
            raise RuntimeError(f"ServiceNow API error {r.status_code}")
        payload = r.json()
        chunk = payload.get("result", []) or []
        logging.debug(f"Fetched {len(chunk)} rows in this batch")
        if not chunk:
            break
        total += len(chunk)
        if len(chunk) < page_size:
            break
        offset += page_size
        time.sleep(0.1)
    logging.info(f"Total records counted: {total}")
    return total

# ---------------------------
# Data Streamer (NDJSON in memory)
# ---------------------------
def stream_servicenow_data_as_ndjson(table, start_ts, end_ts, token, instance, page_size=PAGE_SIZE):
    start = iso_to_utc(start_ts) if isinstance(start_ts, str) else start_ts
    end   = iso_to_utc(end_ts) if isinstance(end_ts, str) else end_ts

    logging.info(f"Streaming records for {table} between {start} and {end} (UTC)")

    url = f"https://{instance}.service-now.com/api/now/table/{table}"
    headers = {"Accept": "application/json", "Authorization": f"Bearer {token}"}
    query = build_query(start, end)

    buffer = io.StringIO()
    offset, total = 0, 0
    while True:
        params = {
            "sysparm_query": query,
            "sysparm_limit": page_size,
            "sysparm_offset": offset,
            "sysparm_display_value": "true",
            "sysparm_exclude_reference_link": "true"
        }
        logging.debug(f"Request URL: {url} with params={params}")
        r = requests.get(url, headers=headers, params=params, timeout=60)
        logging.debug(f"HTTP {r.status_code}, response length={len(r.text)}")
        if r.status_code != 200:
            logging.error(f"Error {r.status_code}: {r.text[:500]}")
            raise RuntimeError(f"ServiceNow API error {r.status_code}")
        payload = r.json()
        chunk = payload.get("result", []) or []
        if not chunk:
            break
        for rec in chunk:
            line = json.dumps(rec, ensure_ascii=False)
            buffer.write(line + "\n")
            logging.debug(f"Record written: {line}")  # full record logged
        total += len(chunk)
        logging.debug(f"Fetched {len(chunk)} rows (total so far: {total})")
        if len(chunk) < page_size:
            break
        offset += page_size
        time.sleep(0.1)

    logging.info(f"Total records streamed: {total}")
    buffer.seek(0)
    return buffer, start, end

# ---------------------------
# Wrappers
# ---------------------------
def get_only_the_source_count(config, record):
    return get_the_count_from_servicenow_for_table(
        config["table"],
        record["start_ts"],
        record["end_ts"],
        config["token"],
        config["instance_name"]
    )

def transfer_data_from_serviceNow_to_aws_s3(record, config):
    buffer, start, end = stream_servicenow_data_as_ndjson(
        config["table"],
        record["start_ts"],
        record["end_ts"],
        config["token"],
        config["instance_name"]
    )

    # Build filename: {table}_{start}_{end}_{epoch}.json
    start_str = sanitize_for_filename(start)
    end_str   = sanitize_for_filename(end)
    epoch     = int(datetime.now(timezone.utc).timestamp())
    key = f"{config['table']}_{start_str}_{end_str}_{epoch}.json"

    logging.info(f"Uploading to S3: s3://{config['s3_bucket']}/{key}")
    s3 = boto3.client("s3")
    s3.upload_fileobj(buffer, config["s3_bucket"], key)

    return f"s3://{config['s3_bucket']}/{key}"

# ---------------------------
# Example Run
# ---------------------------
if __name__ == "__main__":
    config = {
        "instance_name": "synopsys",
        "token": "YOUR_TOKEN",
        "table": "sys_history_line",
        "s3_bucket": "my-bucket"
    }

    record = {
        "start_ts": "2025-09-22T06:45:32-07:00",
        "end_ts":   "2025-09-22T07:45:32-07:00"
    }

    count = get_only_the_source_count(config, record)
    print("Count:", count)

    s3_path = transfer_data_from_serviceNow_to_aws_s3(record, config)
    print("Uploaded to:", s3_path)
