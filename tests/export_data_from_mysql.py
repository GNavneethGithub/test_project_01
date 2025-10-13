from typing import Dict, List
import io
import re
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import create_engine, text

VALID_IDENT_RE = re.compile(r"^[A-Za-z0-9_]+$")


def _validate_ident(name: str):
    if not VALID_IDENT_RE.match(name):
        raise ValueError(f"invalid identifier: {name}")


def export_mysql_table_to_s3(
    mysql_config: Dict,
    aws_s3_config: Dict,
    start_ts: str,
    end_ts: str,
    table_name: str,
    pk_cols: List[str],
    ts_col: str,
    max_part_bytes: int = 250 * 1024 * 1024,
    sample_rows: int = 1000,
) -> List[str]:
    """
    Stream rows from MySQL (time-filtered by ts_col) into in-memory Parquet parts and upload to S3.
    Inputs:
      - mysql_config: must contain host, user, password, database; optional port, driver
      - aws_s3_config: must contain bucket; optional prefix, aws_access_key_id, aws_secret_access_key, region_name, endpoint_url
      - start_ts, end_ts: ISO timezone-aware strings (passed to DB as params)
      - table_name: table to read
      - pk_cols: list of primary key column names (not used internally here except validation; kept for user's downstream needs)
      - ts_col: timestamp column name to filter/order by
    Returns list of uploaded S3 object keys.
    """
    # validate identifiers to avoid SQL injection
    _validate_ident(table_name)
    _validate_ident(ts_col)
    for c in pk_cols:
        _validate_ident(c)

    driver = mysql_config.get("driver", "pymysql")
    user = mysql_config["user"]
    pw = mysql_config["password"]
    host = mysql_config["host"]
    port = mysql_config.get("port", 3306)
    database = mysql_config.get("database") or mysql_config.get("db")
    engine_url = f"mysql+{driver}://{user}:{pw}@{host}:{port}/{database}"
    engine = create_engine(engine_url, pool_pre_ping=True)

    # safe-quoted identifiers (we validated with regex)
    tq = f"`{table_name}`"
    tsq = f"`{ts_col}`"

    base_sql = f"SELECT * FROM {tq} WHERE {tsq} BETWEEN :start_ts AND :end_ts ORDER BY {tsq} ASC"

    # prepare S3
    s3 = boto3.client(
        "s3",
        aws_access_key_id=aws_s3_config.get("aws_access_key_id"),
        aws_secret_access_key=aws_s3_config.get("aws_secret_access_key"),
        region_name=aws_s3_config.get("region_name"),
        endpoint_url=aws_s3_config.get("endpoint_url"),
    )
    bucket = aws_s3_config["bucket"]
    prefix = aws_s3_config.get("prefix", "").rstrip("/")
    uploaded_keys: List[str] = []

    # sample to estimate bytes/row
    sample_df = pd.DataFrame()
    sample_sql = base_sql + f" LIMIT {int(sample_rows)}"
    sample_df = pd.read_sql_query(text(sample_sql), engine, params={"start_ts": start_ts, "end_ts": end_ts})
    if sample_df.empty:
        return []

    buf = io.BytesIO()
    sample_df.to_parquet(buf, engine="pyarrow", index=False)
    avg_bytes_per_row = max(1, buf.getbuffer().nbytes / max(1, len(sample_df)))

    est_rows_per_part = max(1, int(max_part_bytes / avg_bytes_per_row))
    read_chunksize = max(100, min(est_rows_per_part, 200_000))

    def _make_key(part_idx: int):
        safe_start = start_ts.replace(":", "").replace("+", "_").replace(" ", "_")
        safe_end = end_ts.replace(":", "").replace("+", "_").replace(" ", "_")
        base = f"{table_name}_{safe_start}_{safe_end}_part{part_idx}.parquet"
        return f"{prefix}/{base}" if prefix else base

    part_idx = 0
    buffer = None
    writer = None

    # iterate chunks
    iterator = pd.read_sql_query(text(base_sql), engine, params={"start_ts": start_ts, "end_ts": end_ts}, chunksize=read_chunksize)
    for chunk_df in iterator:
        if chunk_df.empty:
            continue
        table = pa.Table.from_pandas(chunk_df, preserve_index=False)
        if writer is None:
            buffer = io.BytesIO()
            writer = pq.ParquetWriter(buffer, table.schema, compression="snappy")
        writer.write_table(table)

        # check buffer size and flush if threshold reached
        if buffer.getbuffer().nbytes >= max_part_bytes:
            writer.close()
            buffer.seek(0)
            key = _make_key(part_idx)
            s3.upload_fileobj(buffer, bucket, key)
            uploaded_keys.append(key)
            part_idx += 1
            buffer.close()
            buffer = None
            writer = None

    # flush remaining
    if writer is not None and buffer is not None:
        writer.close()
        buffer.seek(0)
        key = _make_key(part_idx)
        s3.upload_fileobj(buffer, bucket, key)
        uploaded_keys.append(key)
        buffer.close()

    return uploaded_keys








# # export_10_local.py
# import os
# from sqlalchemy import create_engine
# import pandas as pd

# # ---------- CONFIG ----------
# DB_HOST = "xyz-db-1"        # change to 'localhost' if using SSH tunnel
# DB_PORT = 3306               # change to forwarded port if tunneled (eg 33306)
# DB_USER = "xyz_guest"
# DB_NAME = "xyz_datawarehouse"
# TABLES = ["xyz_tbl1", "xyz_tbl2"]   # we need to take one table at a time
# LIMIT = 10
# LOCAL_DIR = os.path.abspath("./data/xyz")
# # ---------- DB PASSWORD (hardcoded) ----------
# DB_PASSWORD = "PutYourPasswordHere"  # or set to None and export from env var below
# # ------------------------------------------------

# if not DB_PASSWORD:
#     raise SystemExit("DB_PASSWORD not set in script")

# os.makedirs(LOCAL_DIR, exist_ok=True)

# engine_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
# engine = create_engine(engine_url, pool_pre_ping=True)

# def export_table_to_local(table):
#     sql = f"SELECT * FROM `{table}` LIMIT {LIMIT}"
#     df = pd.read_sql_query(sql, engine)
#     if df.empty:
#         print(f"[WARN] {table} returned 0 rows")
#         return
#     out_path = os.path.join(LOCAL_DIR, f"{table}.parquet")
#     df.to_parquet(out_path, engine="pyarrow", index=False)
#     print(f"WROTE {len(df)} rows -> {out_path}")

# if __name__ == "__main__":
#     for t in TABLES:
#         try:
#             export_table_to_local(t)
#         except Exception as e:
#             print(f"[ERROR] {t}: {e}")
