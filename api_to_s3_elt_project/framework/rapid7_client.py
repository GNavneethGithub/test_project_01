# framework/rapid7_client.py
from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Dict, List, Union

import requests

ResultType = Union[Dict[str, Any], List[Any]]

logger = logging.getLogger(__name__)


def _get_api_key(api_config: Dict[str, Any]) -> str:
    key = api_config.get("api_key")
    if key:
        return key

    env_var = api_config.get("api_key_env", "RAPID7_API_KEY")
    key = os.environ.get(env_var)
    if not key:
        raise RuntimeError(
            f"Rapid7 API key not found. "
            f"Set api_config['api_key'] or environment variable {env_var}."
        )
    return key


def _rapid7_request(api_config: Dict[str, Any], payload: Dict[str, Any]) -> Dict[str, Any]:
    endpoint = api_config["endpoint"]
    api_key = _get_api_key(api_config)
    timeout = int(api_config.get("http_timeout_sec", 60))

    headers = {
        "Content-Type": "application/json",
        "X-Api-Key": api_key,
    }

    logger.debug("Rapid7 request -> %s (payload keys: %s)", endpoint, list(payload.keys()))
    resp = requests.post(endpoint, headers=headers, json=payload, timeout=timeout)
    logger.debug("Rapid7 response status: %s", resp.status_code)
    resp.raise_for_status()

    try:
        data = resp.json()
    except json.JSONDecodeError:
        logger.error("Invalid JSON from Rapid7 (first 500 chars): %s", resp.text[:500])
        raise RuntimeError("Invalid JSON from Rapid7")

    if "errors" in data:
        logger.error("Rapid7 GraphQL errors: %s", data["errors"])
        raise RuntimeError(f"Rapid7 GraphQL error(s): {data['errors']}")

    return data


def sanity_check(api_config: Dict[str, Any]) -> None:
    logger.info("Rapid7 sanity check: __typename")
    payload = {"query": "query { __typename }"}
    resp = _rapid7_request(api_config, payload)
    typename = resp.get("data", {}).get("__typename")
    if typename != "Query":
        raise RuntimeError(f"Sanity check failed: __typename={typename!r}")
    logger.info("Rapid7 sanity check OK")


def create_export_job(api_config: Dict[str, Any]) -> str:
    logger.info("Creating Rapid7 vulnerability export job...")
    query = """
    mutation CreateVulnExport {
      createVulnerabilityExport(input: {}) {
        id
      }
    }
    """
    resp = _rapid7_request(api_config, {"query": query})
    try:
        export_id = resp["data"]["createVulnerabilityExport"]["id"]
    except KeyError:
        logger.error("Unexpected response creating export job: %s", resp)
        raise RuntimeError("Unable to parse export ID from Rapid7 response")
    logger.info("Created export job id=%s", export_id)
    return export_id


def poll_until_ready(api_config: Dict[str, Any], export_id: str) -> Dict[str, Any]:
    poll_interval = int(api_config.get("poll_interval_sec", 30))
    timeout_sec = int(api_config.get("export_timeout_sec", 3600))

    query = (
        'query GetExport { '
        f'export(id: "{export_id}") {{ '
        'id status result { urls prefix } '
        '} '
        '}'
    )

    logger.info(
        "Polling Rapid7 export id=%s (timeout=%ss, interval=%ss)",
        export_id,
        timeout_sec,
        poll_interval,
    )
    start = time.time()

    while True:
        resp = _rapid7_request(api_config, {"query": query})
        exp = resp.get("data", {}).get("export")

        if not exp:
            raise RuntimeError("No 'export' object in Rapid7 response")

        status = (exp.get("status") or "").upper()
        logger.info("Rapid7 export status=%s", status)

        if status in {"READY", "COMPLETED", "SUCCEEDED"}:
            logger.info("Rapid7 export completed.")
            return exp.get("result") or {}

        if status in {"FAILED", "ERROR"}:
            logger.error("Rapid7 export failed: %s", exp)
            raise RuntimeError(f"Rapid7 export failed: {json.dumps(exp)}")

        if time.time() - start > timeout_sec:
            raise RuntimeError("Timeout waiting for Rapid7 export")

        time.sleep(poll_interval)


def extract_links(result: ResultType) -> List[Dict[str, str]]:
    """
    Normalize result into list of {"url": "..."} or {"prefix": "s3://..."}.
    """
    records: List[Dict[str, str]] = []

    if isinstance(result, dict):
        urls = result.get("urls")
        prefix = result.get("prefix")

        if isinstance(urls, list) and urls:
            for u in urls:
                if isinstance(u, str) and u.strip():
                    records.append({"url": u})
        elif isinstance(prefix, str) and prefix.strip():
            records.append({"prefix": prefix})

        logger.info("Extracted %d records from dict result", len(records))
        return records

    if isinstance(result, list):
        # list[str]
        if all(isinstance(x, str) for x in result):
            for u in result:
                if u and u.strip():
                    records.append({"url": u})
            logger.info("Extracted %d records from list[str]", len(records))
            return records

        # list[dict]
        for item in result:
            if not isinstance(item, dict):
                continue
            urls = item.get("urls")
            if isinstance(urls, list) and urls:
                for u in urls:
                    if isinstance(u, str) and u.strip():
                        records.append({"url": u})
                continue
            prefix = item.get("prefix")
            if isinstance(prefix, str) and prefix.strip():
                records.append({"prefix": prefix})
                continue
            for v in item.values():
                if isinstance(v, str) and (
                    v.startswith("http://")
                    or v.startswith("https://")
                    or v.startswith("s3://")
                ):
                    rec = {"url": v} if v.startswith("http") else {"prefix": v}
                    if rec not in records:
                        records.append(rec)

        logger.info("Extracted %d records from list[dict]", len(records))
        return records

    raise RuntimeError(f"Unexpected result shape: {type(result)}")


def run_rapid7_export(api_config: Dict[str, Any]) -> List[Dict[str, str]]:
    """
    High-level Task 1:
      sanity_check -> create_export_job -> poll_until_ready -> extract_links
    Returns: list[{"url": ...} | {"prefix": ...}]
    """
    sanity_check(api_config)
    export_id = create_export_job(api_config)
    result = poll_until_ready(api_config, export_id)
    records = extract_links(result)
    logger.info("Rapid7 export produced %d records", len(records))
    return records
