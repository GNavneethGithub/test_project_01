import boto3

s3 = boto3.client('s3')

url = s3.generate_presigned_url(
    'get_object',
    Params={'Bucket': 'your-bucket-name', 'Key': 'your/file/path'},
    ExpiresIn=3600
)

print(url)



import requests
import boto3

presigned_url = "https://your-presigned-url-here"
target_bucket = "your-target-bucket"
target_key = "your/prefix/filename.pdf"  # Example: "uploads/2025/report.pdf"

# Step 1: Download from the presigned URL
response = requests.get(presigned_url)

# Step 2: Upload to your S3 bucket
s3 = boto3.client("s3")
s3.put_object(
    Bucket=target_bucket,
    Key=target_key,
    Body=response.content
)

print("File uploaded to s3://%s/%s" % (target_bucket, target_key))





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


if __name__ == "__main__":
    main()

