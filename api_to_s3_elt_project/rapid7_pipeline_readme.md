- Rapid7 export task calls the API, creates an export job, polls until completion, and normalizes the result into in-memory records.
- Each record is either a URL to download or an S3 prefix to copy, never written to disk as a file.
- The first Airflow task (`rapid7_export`) executes the Rapid7 export wrapper and pushes the list of records to XCom.
- The second Airflow task (`s3_transfer`) pulls the records from XCom and passes them, along with the config, into the S3 transfer wrapper.
- The S3 transfer wrapper uses a thread pool to process multiple records in parallel within a single Airflow task.
- URL records are downloaded over HTTPS and streamed directly into the configured AWS S3 bucket and target prefix.
- Prefix records are copied server-side from the source S3 prefix into the configured AWS S3 bucket and target prefix.
- If any worker thread encounters an error during transfer, remaining tasks are cancelled and the target S3 prefix is cleaned to avoid partial data.
- All API endpoints, credentials, target locations, timeouts, and concurrency (max_workers) are driven entirely from the per-project JSON config.
- The shared framework code is reusable across multiple projects, while each project only needs its own config.json and DAG wiring.

# Rapid7 → S3 Framework: Design & Behavior

## 1. Does this project work as expected?

From a structural and Python perspective, the project is sound and should behave as intended **provided** that:

- The `framework` package is on `PYTHONPATH` in your Airflow environment (so imports like `from framework...` work).
- Rapid7 credentials and endpoint in `config["api_"]` are valid.
- AWS credentials/role in `config["aws"]` (or environment/IAM) have permission to download from any Rapid7 S3 prefixes and write to your target S3 bucket.
- XCom size limits are not exceeded by the list of records (usually fine unless there are hundreds of thousands of records).

I can’t actually call the real Rapid7 or AWS services from here, but statically:
- All imports, function signatures, and data flows line up correctly.
- The two-task Airflow DAG wiring is consistent with the framework APIs.
- The in-memory contract (`records: List[Dict[str, str]]`) is consistent across export and transfer layers.

Below is a walkthrough of the flow and where to watch for issues in a real deployment.

## 2. High-level Flow

1. **Airflow Task 1 – `rapid7_export`**
   - Loads `config.json` for the project.
   - Uses `rapid7_export_task(config)` (from `framework/tasks/rapid7_export_task.py`).
   - Internally calls `run_rapid7_export(api_config)` in `rapid7_client.py`.
   - Steps inside:
     1. `sanity_check(api_config)` → runs `query { __typename }` to verify API + credentials.
     2. `create_export_job(api_config)` → triggers Rapid7 vulnerability export and returns `export_id`.
     3. `poll_until_ready(api_config, export_id)` → polls until status is READY/COMPLETED/SUCCEEDED (or fails/timeout).
     4. `extract_links(result)` → normalizes Rapid7’s `result` into a list of `{"url": ...}` / `{"prefix": "s3://..."}`.
   - Returns this list of records to Airflow, which stores it in XCom.

2. **Airflow Task 2 – `s3_transfer`**
   - Pulls `records` from XCom with `ti.xcom_pull(task_ids='rapid7_export')`.
   - Calls `s3_transfer_task(config, records)` (from `framework/tasks/s3_transfer_task.py`).
   - Internally calls `transfer_records_parallel(aws_config, records)` in `s3_transfer_parallel.py`.

3. **Parallel transfer inside Task 2**
   - Reads `target_bucket`, `target_prefix`, `max_workers`, and timeouts from `config["aws"]`.
   - Creates a `ThreadPoolExecutor(max_workers=max_workers)`.
   - Submits one worker function per record:
     - If record has `"url"` → `_upload_url_record`:
       - Streams HTTP response (`requests.get(stream=True)`) directly into S3 via `upload_fileobj`.
     - If record has `"prefix"` → `_copy_prefix_record`:
       - Parses `s3://bucket/prefix` and copies all objects under `prefix` into your target bucket/prefix using S3 `copy`.
   - Logs each worker’s completion or error.

4. **Failure handling & cleanup**
   - If any worker raises an exception:
     - The loop breaks, `error_happened = True`.
     - Remaining futures are cancelled.
     - If `target_prefix` is non-empty, `delete_s3_prefix` is called to delete all objects under `s3://target_bucket/target_prefix` for this run.
     - Task 2 raises `RuntimeError`, so Airflow marks `s3_transfer` as failed.
   - If no worker fails, the function logs success and exits normally.

## 3. Config-driven Behavior

Each project has its own `config.json`, e.g. `users/project_01/config.json`, with a structure like:

```json
{
  "api_": {
    "endpoint": "https://us.api.insight.rapid7.com/export/graphql",
    "api_key": null,
    "api_key_env": "RAPID7_API_KEY_PROJECT_01",
    "poll_interval_sec": 30,
    "export_timeout_sec": 3600,
    "http_timeout_sec": 60
  },
  "aws": {
    "target_bucket": "proj01-target-bucket",
    "target_prefix": "proj01/rapid7/data",
    "region": "us-east-1",
    "profile": null,
    "access_key_id": null,
    "secret_access_key": null,
    "session_token": null,
    "max_workers": 16,
    "http_download_timeout_sec": 300
  },
  "logging": {
    "level": "INFO"
  }
}
```

Key points:

- **Different projects** can have different buckets, prefixes, credentials, timeouts, and `max_workers` without changing framework code.
- If `api_key` is `null`, the framework falls back to `api_key_env` for that project (e.g. `RAPID7_API_KEY_PROJECT_01`).
- If AWS keys are omitted, boto3 will use IAM roles / environment credentials.

## 4. Logging & Debuggability

- `logging_setup.setup_logging(config)` sets a global logging level and format.
- Each module (`rapid7_client`, `s3_transfer_parallel`, task wrappers) gets its own logger via `logging.getLogger(__name__)`.
- You log:
  - Rapid7 request status and statuses of export jobs.
  - Number of records extracted from the result.
  - Start and end of each pipeline step.
  - Per-worker success messages (what was transferred where).
  - Detailed stack traces on errors (`logger.exception` in worker loop).
  - Cleanup events (how many objects were deleted from S3).

This is good for debugging runs later via Airflow’s log UI.

## 5. Things to Double-check When You Deploy

1. **Python import paths**
   - Ensure the `framework` directory is on `PYTHONPATH` inside the Airflow container so `from framework...` imports work.
2. **XCom size**
   - If Rapid7 returns a *huge* number of records, the list may be large.
   - If that happens, you might want a different strategy (e.g. store a manifest in S3 instead of XCom), but for most normal volumes it should be fine.
3. **Permissions**
   - AWS principal running Airflow must have:
     - `s3:GetObject` on Rapid7’s S3 bucket/prefix (if using `prefix`-style records).
     - `s3:PutObject`, `s3:DeleteObject`, and `s3:ListBucket` on your target bucket.
4. **Target prefix semantics**
   - Cleanup deletes everything under `target_prefix` on failure.
   - That prefix should be dedicated to this pipeline (per project) to avoid accidental deletion of unrelated data.

## 6. Summary

Conceptually, the project matches what you described:

- Shared **framework** for export + parallel S3 transfer.
- Per-project **config.json** and **DAG** wiring.
- Data stays **in memory**, passed via XCom, no local temp files.
- Parallelism is handled **inside Task 2** using a thread pool with configurable `max_workers`.
- Robust, config-driven cleanup to avoid leaving partial or corrupt data in your target S3 prefix.

From a code-structure and flow point of view, it’s in good shape and ready to be dropped into an Airflow environment with valid configs and credentials.
