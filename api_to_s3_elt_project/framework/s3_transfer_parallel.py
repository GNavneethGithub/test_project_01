# framework/s3_transfer_parallel.py
from __future__ import annotations



import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from typing import Any, Dict, List

import boto3
import requests
from botocore.client import BaseClient
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


def build_s3_client(aws_config: Dict[str, Any]) -> BaseClient:
    session_kwargs: Dict[str, Any] = {}
    if aws_config.get("profile"):
        session_kwargs["profile_name"] = aws_config["profile"]

    session = boto3.Session(**session_kwargs) if session_kwargs else boto3.Session()

    client_kwargs: Dict[str, Any] = {}
    if aws_config.get("region"):
        client_kwargs["region_name"] = aws_config["region"]

    if aws_config.get("access_key_id") and aws_config.get("secret_access_key"):
        client_kwargs["aws_access_key_id"] = aws_config["access_key_id"]
        client_kwargs["aws_secret_access_key"] = aws_config["secret_access_key"]
        if aws_config.get("session_token"):
            client_kwargs["aws_session_token"] = aws_config["session_token"]

    return session.client("s3", **client_kwargs)


def parse_s3_uri(s3_uri: str) -> tuple[str, str]:
    if not s3_uri.startswith("s3://"):
        raise ValueError(f"Not an s3 uri: {s3_uri}")
    stripped = s3_uri[5:]
    parts = stripped.split("/", 1)
    bucket = parts[0]
    prefix = parts[1] if len(parts) > 1 else ""
    return bucket, prefix



def build_weekly_prefix(base_prefix: str | None, run_date: date) -> str:
    """
    Build final S3 prefix like:
      <base_prefix>/<ISO_YEAR>/week_<ISO_WEEK>/

    Example:
      base_prefix = "proj01/rapid7/data"
      run_date = 2025-03-05 (ISO week 10)
      => "proj01/rapid7/data/2025/week_10"
    """
    iso_year, iso_week, _ = run_date.isocalendar()

    parts: List[str] = []
    if base_prefix:
        parts.append(base_prefix.rstrip("/"))

    parts.append(f"{iso_year:04d}")
    parts.append(f"week_{iso_week:02d}")

    return "/".join(parts)



def prefix_has_data(aws_config: Dict[str, Any], bucket: str, prefix: str) -> bool:
    """
    Return True if there is at least one object under s3://bucket/prefix.
    Used to skip reloading an already-processed week.
    """
    s3 = build_s3_client(aws_config)
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    has_data = "Contents" in resp
    logger.info(
        "Checked existing data for s3://%s/%s -> %s",
        bucket,
        prefix,
        "FOUND" if has_data else "NOT FOUND",
    )
    return has_data


def delete_s3_prefix(aws_config: Dict[str, Any], bucket: str, prefix: str) -> int:
    """
    Delete all objects under s3://bucket/prefix.
    """
    if not prefix:
        logger.warning(
            "delete_s3_prefix called with empty prefix – refusing to delete entire bucket %s",
            bucket,
        )
        return 0

    s3 = build_s3_client(aws_config)
    logger.warning("Cleaning s3://%s/%s ...", bucket, prefix)

    paginator = s3.get_paginator("list_objects_v2")
    deleted = 0

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        objs = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if not objs:
            continue
        for i in range(0, len(objs), 1000):
            chunk = objs[i : i + 1000]
            resp = s3.delete_objects(Bucket=bucket, Delete={"Objects": chunk})
            deleted += len(resp.get("Deleted", []))

    logger.warning(
        "Cleanup complete for s3://%s/%s – deleted %d objects",
        bucket,
        prefix,
        deleted,
    )
    return deleted


def _upload_url_record(
    aws_config: Dict[str, Any],
    record: Dict[str, str],
    target_bucket: str,
    weekly_prefix: str,
) -> str:
    timeout = int(aws_config.get("http_download_timeout_sec", 120))

    url = record["url"]
    parsed = urlparse(url)
    filename = os.path.basename(parsed.path) or parsed.netloc
    dest_key = weekly_prefix.rstrip("/") + "/" + filename

    s3 = build_s3_client(aws_config)

    logger.info("Downloading %s -> s3://%s/%s", url, target_bucket, dest_key)
    resp = requests.get(url, stream=True, timeout=timeout)
    resp.raise_for_status()

    s3.upload_fileobj(resp.raw, Bucket=target_bucket, Key=dest_key)
    resp.close()

    return f"url_to_s3:{url} -> s3://{target_bucket}/{dest_key}"


def _copy_prefix_record(
    aws_config: Dict[str, Any],
    record: Dict[str, str],
    target_bucket: str,
    weekly_prefix: str,
) -> str:
    s3_uri = record["prefix"]
    src_bucket, src_prefix = parse_s3_uri(s3_uri)
    # We keep the structure under weekly_prefix
    dest_prefix = weekly_prefix.rstrip("/")

    s3 = build_s3_client(aws_config)
    paginator = s3.get_paginator("list_objects_v2")
    copied = 0

    for page in paginator.paginate(Bucket=src_bucket, Prefix=src_prefix):
        for obj in page.get("Contents", []):
            src_key = obj["Key"]
            suffix = src_key[len(src_prefix):].lstrip("/")
            dest_key = dest_prefix + ("/" + suffix if suffix else "")

            s3.copy(
                {"Bucket": src_bucket, "Key": src_key},
                target_bucket,
                dest_key,
            )
            copied += 1

    logger.info(
        "Copied %d objects from s3://%s/%s -> s3://%s/%s",
        copied,
        src_bucket,
        src_prefix,
        target_bucket,
        dest_prefix,
    )
    return f"prefix_to_s3:{s3_uri} -> s3://{target_bucket}/{dest_prefix} ({copied} objects)"


def transfer_records_parallel(
    aws_config: Dict[str, Any],
    records: List[Dict[str, str]],
    run_date: date,
) -> None:
    """
    Task 2 logic (weekly):
      - Build final weekly prefix based on run_date (year/month/week).
      - If data already exists under that prefix -> skip.
      - Else run parallel transfers using ThreadPoolExecutor.
      - On any failure:
          - cancel remaining
          - clean that weekly prefix
          - raise error
    """
    target_bucket = aws_config.get("target_bucket")
    if not target_bucket:
        raise ValueError("aws_config['target_bucket'] must be provided")

    base_prefix = aws_config.get("target_prefix") or ""
    weekly_prefix = build_weekly_prefix(base_prefix, run_date)
    max_workers = int(aws_config.get("max_workers", 8))

    logger.info(
        "Weekly S3 transfer to s3://%s/%s (run_date=%s, max_workers=%d)",
        target_bucket,
        weekly_prefix,
        run_date.isoformat(),
        max_workers,
    )

    # Idempotency: skip if this week's data already loaded
    if prefix_has_data(aws_config, target_bucket, weekly_prefix):
        logger.info(
            "Data already exists for this week at s3://%s/%s – skipping transfer.",
            target_bucket,
            weekly_prefix,
        )
        return

    def worker(rec: Dict[str, str]) -> str:
        if "url" in rec:
            return _upload_url_record(aws_config, rec, target_bucket, weekly_prefix)
        if "prefix" in rec:
            return _copy_prefix_record(aws_config, rec, target_bucket, weekly_prefix)
        logger.warning("Unknown record type, skipping: %s", rec)
        return "skipped"

    futures = []
    error_happened = False
    first_exc: Exception | None = None

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for rec in records:
            futures.append(executor.submit(worker, rec))

        for fut in as_completed(futures):
            try:
                msg = fut.result()
                logger.info("Worker finished: %s", msg)
            except Exception as e:
                logger.exception("Error in S3 transfer worker")
                error_happened = True
                first_exc = e
                break

        if error_happened:
            logger.warning("Error detected – cancelling remaining S3 tasks")
            for f in futures:
                f.cancel()

    if error_happened:
        logger.warning(
            "Cleaning weekly prefix due to failure: s3://%s/%s",
            target_bucket,
            weekly_prefix,
        )
        delete_s3_prefix(aws_config, target_bucket, weekly_prefix)
        raise RuntimeError("S3 transfer failed; weekly prefix cleaned") from first_exc

    logger.info("Weekly parallel S3 transfer completed with no errors.")
