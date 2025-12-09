

#!/usr/bin/env python3

import os
import time
import json
import subprocess

API_KEY = os.environ.get("RAPID7_API_KEY", "<YOUR_API_KEY>")
ENDPOINT = "https://us.api.insight.rapid7.com/export/graphql"
OUTPUT_FILE = "links.ndjson"
POLL_INTERVAL = 30
TIMEOUT = 3600


def run_curl(payload_json):
    """Run curl with JSON payload, return parsed JSON."""
    cmd = [
        "curl", "-s", ENDPOINT,
        "-H", "Content-Type: application/json",
        "-H", f"X-Api-Key: {API_KEY}",
        "-d", json.dumps(payload_json)
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    out = result.stdout.strip()

    if not out:
        raise RuntimeError("Empty response from curl")

    try:
        return json.loads(out)
    except json.JSONDecodeError:
        raise RuntimeError(f"Invalid JSON from curl:\n{out}")


def sanity_check():
    print("Checking API...")
    resp = run_curl({"query": "query { __typename }"})
    if resp.get("data", {}).get("__typename") != "Query":
        raise RuntimeError("Sanity check failed")
    print("API OK.\n")


def create_export_job():
    print("Creating export job...")
    query = """
    mutation CreateVulnExport {
      createVulnerabilityExport(input: {}) {
        id
      }
    }
    """
    resp = run_curl({"query": query})
    export_id = resp["data"]["createVulnerabilityExport"]["id"]
    print("Created export ID:", export_id, "\n")
    return export_id

def poll_until_ready(export_id):
    # Build a query that inlines the export_id directly (no GraphQL variables)
    # This avoids the "Unknown type 'ID'" error some endpoints return.
    query_template = (
        'query GetExport { '
        'export(id: "%s") { '
        'id status result { urls prefix } '
        '} '
        '}'
    )
    query = query_template % export_id

    start = time.time()

    while True:
        resp = run_curl({"query": query})
        exp = resp.get("data", {}).get("export")

        if not exp:
            raise RuntimeError("Unexpected response:\n" + json.dumps(resp))

        status = (exp.get("status") or "").upper()
        print("Status:", status)

        if status in ("READY", "COMPLETED", "SUCCEEDED"):
            print("Export completed.\n")
            return exp.get("result") or {}

        if status in ("FAILED", "ERROR"):
            raise RuntimeError("Export failed:\n" + json.dumps(exp))

        if time.time() - start > TIMEOUT:
            raise RuntimeError("Timeout waiting for export\n" + json.dumps(resp))

        time.sleep(POLL_INTERVAL)

def extract_links(result):
    """
    Return a list of records for NDJSON:
      [{"url": "..."}, {"url": "..."}]  OR  [{"prefix":"s3://..."}]
    Handles multiple shapes:
      - result is a dict: { "urls": [...], "prefix": "..." }
      - result is a list of dicts: [ { "urls": [...] }, { "prefix": "..." } ]
      - result is a list of strings: [ "https://...file1", "https://...file2" ]
    """
    records = []

    # Case A: result is a dict
    if isinstance(result, dict):
        urls = result.get("urls")
        prefix = result.get("prefix")
        if isinstance(urls, list) and urls:
            for u in urls:
                if isinstance(u, str) and u.strip():
                    records.append({"url": u})
        elif prefix:
            records.append({"prefix": prefix})
        return records

    # Case B: result is a list
    if isinstance(result, list):
        # If it's a list of strings (direct URLs)
        if all(isinstance(item, str) for item in result):
            for u in result:
                if u and u.strip():
                    records.append({"url": u})
            return records

        # If it's a list of dicts, try to pull urls/prefix from each dict
        for item in result:
            if not isinstance(item, dict):
                continue
            # collect urls if present
            urls = item.get("urls") if isinstance(item.get("urls", None), list) else None
            if urls:
                for u in urls:
                    if isinstance(u, str) and u.strip():
                        records.append({"url": u})
                continue
            # collect prefix if present
            prefix = item.get("prefix")
            if prefix:
                records.append({"prefix": prefix})
                continue
            # fallback: scan dict values for strings that look like s3/http links
            for v in item.values():
                if isinstance(v, str) and (v.startswith("http://") or v.startswith("https://") or v.startswith("s3://")):
                    # avoid duplicates
                    rec = {"url": v} if v.startswith("http") else {"prefix": v}
                    if rec not in records:
                        records.append(rec)
        return records

    # If we get here, we don't know the shape
    raise RuntimeError(f"Unexpected result shape: {type(result)} - please inspect the debug output")

def write_ndjson(records):
    with open(OUTPUT_FILE, "w") as f:
        for rec in records:
            f.write(json.dumps(rec) + "\n")

    print(f"Saved {len(records)} records to {OUTPUT_FILE}\n")


def main():
    if not API_KEY or API_KEY == "<YOUR_API_KEY>":
        print("ERROR: Set RAPID7_API_KEY or edit API_KEY in script.")
        return

    sanity_check()
    export_id = create_export_job()
    result = poll_until_ready(export_id)
    records = extract_links(result)
    write_ndjson(records)

    print("DONE — Only file created:", OUTPUT_FILE)



# ======= PART 2: send links.ndjson content to your AWS S3 bucket =======
# Replace target_bucket and optional target_prefix with your values.
# Ensure AWS creds are available (AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY or IAM role)

import boto3
import requests
from urllib.parse import urlparse

def read_links_ndjson(path="links.ndjson"):
    recs = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            recs.append(json.loads(line))
    return recs

def upload_stream_to_s3(s3_client, bucket, key, stream_resp):
    """
    Upload streaming response content to S3 using upload_fileobj for streaming support.
    """
    from io import BytesIO

    # upload_fileobj expects a file-like object; requests supports iter_content so we
    # accumulate into a small buffer and stream via BytesIO in chunks.
    # For large files consider a different streaming approach (e.g., multipart upload).
    bio = BytesIO()
    for chunk in stream_resp.iter_content(chunk_size=8192):
        if chunk:
            bio.write(chunk)
    bio.seek(0)
    s3_client.upload_fileobj(bio, Bucket=bucket, Key=key)

def download_url_and_upload(s3_client, url, target_bucket, target_key):
    """
    Download from URL (supports presigned HTTP) and upload to S3.
    """
    # Attempt streaming GET
    resp = requests.get(url, stream=True, timeout=120)
    resp.raise_for_status()
    upload_stream_to_s3(s3_client, target_bucket, target_key, resp)

def parse_s3_uri(s3uri: str):
    # s3://bucket/prefix/path -> (bucket, prefix)
    if not s3uri.startswith("s3://"):
        raise ValueError("Not an s3 uri: " + s3uri)
    stripped = s3uri[5:]
    parts = stripped.split("/", 1)
    bucket = parts[0]
    prefix = parts[1] if len(parts) > 1 else ""
    return bucket, prefix

def copy_s3_objects(s3_client, source_bucket, source_key, dest_bucket, dest_key_prefix):
    """
    Copy all objects under source_key (prefix) into dest_bucket under dest_key_prefix.
    Uses server-side copy (CopyObject) - requires permission to read source and write dest.
    """
    paginator = s3_client.get_paginator("list_objects_v2")
    copied = 0
    for page in paginator.paginate(Bucket=source_bucket, Prefix=source_key):
        for obj in page.get("Contents", []):
            src_key = obj["Key"]
            # create destination key by appending the remainder of the key after source_key
            suffix = src_key[len(source_key):].lstrip("/")
            dest_key = (dest_key_prefix.rstrip("/") + "/" + suffix) if suffix else dest_key_prefix.rstrip("/")
            copy_source = {"Bucket": source_bucket, "Key": src_key}
            s3_client.copy(copy_source, dest_bucket, dest_key)
            copied += 1
            print(f"Copied s3://{source_bucket}/{src_key} -> s3://{dest_bucket}/{dest_key}")
    if copied == 0:
        print("No objects found to copy under prefix:", source_key)
    return copied

def send_links_to_s3(links_path="links.ndjson", target_bucket=None, target_prefix=""):
    """
    Main entry: reads links.ndjson and transfers objects to the target S3 bucket.
    - For {"url": "..."} entries: downloads and uploads the file to target_bucket/target_prefix/<filename>
    - For {"prefix": "s3://.../"} entries: performs server-side S3 copy of all objects under that prefix
    """
    if not target_bucket:
        raise ValueError("target_bucket must be provided")

    s3 = boto3.client("s3")
    records = read_links_ndjson(links_path)
    total = len(records)
    print(f"Found {total} records in {links_path}")
 
    for rec in records:
        if "url" in rec:
            url = rec["url"]
            # choose a target key name: use filename from URL, prefixed by target_prefix
            parsed = urlparse(url)
            filename = os.path.basename(parsed.path) or parsed.netloc
            dest_key = (target_prefix.rstrip("/") + "/" + filename) if target_prefix else filename
            print(f"Downloading {url} and uploading to s3://{target_bucket}/{dest_key} ...")
            try:
                download_url_and_upload(s3, url, target_bucket, dest_key)
                print("Uploaded successfully.")
            except Exception as e:
                print(f"ERROR transferring {url} -> s3://{target_bucket}/{dest_key}: {e}")
        elif "prefix" in rec:
            s3uri = rec["prefix"]
            # parse source and copy objects
            src_bucket, src_prefix = parse_s3_uri(s3uri)
            dest_prefix = target_prefix.rstrip("/") if target_prefix else src_prefix.rstrip("/")
            print(f"Copying objects from s3://{src_bucket}/{src_prefix} to s3://{target_bucket}/{dest_prefix} ...")
            try:
                copied = copy_s3_objects(s3, src_bucket, src_prefix, target_bucket, dest_prefix)
                print(f"Copied {copied} objects.")
            except Exception as e:
                print(f"ERROR copying prefix {s3uri} -> s3://{target_bucket}/{dest_prefix}: {e}")
        else:
            print("Unknown record type, skipping:", rec)

    print("Part 2 complete.")







def main():
    # ---------------------------
    # PART 1 — Rapid7 Export
    # ---------------------------
    if not API_KEY or API_KEY == "<YOUR_API_KEY>":
        print("ERROR: Set RAPID7_API_KEY or edit API_KEY in script.")
        return

    print("\n=== PART 1: Rapid7 Export Flow ===\n")

    sanity_check()
    export_id = create_export_job()
    result = poll_until_ready(export_id)
    records = extract_links(result)
    write_ndjson(records)

    print("Rapid7 export completed. links.ndjson generated.\n")

    # ---------------------------
    # PART 2 — Upload to Your AWS S3
    # ---------------------------
    print("\n=== PART 2: Upload to Your AWS S3 ===\n")

    TARGET_BUCKET = "your-target-bucket-name-here"      # <-- CHANGE THIS
    TARGET_PREFIX = "your/target/prefix"                # <-- optional

    send_links_to_s3(
        links_path="links.ndjson",
        target_bucket=TARGET_BUCKET,
        target_prefix=TARGET_PREFIX
    )

    print("\nAll Done ✔️")




if __name__ == "__main__":
    main()








# ======= part 2 =========

# import boto3

# s3 = boto3.client('s3')

# url = s3.generate_presigned_url(
#     'get_object',
#     Params={'Bucket': 'your-bucket-name', 'Key': 'your/file/path'},
#     ExpiresIn=3600
# )

# print(url)

# import requests
# import boto3

# presigned_url = "https://your-presigned-url-here"
# target_bucket = "your-target-bucket"
# target_key = "your/prefix/filename.pdf"  # Example: "uploads/2025/report.pdf"

# # Step 1: Download from the presigned URL
# response = requests.get(presigned_url)

# # Step 2: Upload to your S3 bucket
# s3 = boto3.client("s3")
# s3.put_object(
#     Bucket=target_bucket,
#     Key=target_key,
#     Body=response.content
# )

# print("File uploaded to s3://%s/%s" % (target_bucket, target_key))


# send all the links to my aws s3 bucket

