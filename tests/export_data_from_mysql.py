# export_10_local.py
import os
from sqlalchemy import create_engine
import pandas as pd

# ---------- CONFIG ----------
DB_HOST = "xyz-db-1"        # change to 'localhost' if using SSH tunnel
DB_PORT = 3306               # change to forwarded port if tunneled (eg 33306)
DB_USER = "xyz_guest"
DB_NAME = "xyz_datawarehouse"
TABLES = ["xyz_tbl1", "xyz_tbl2"]
LIMIT = 10
LOCAL_DIR = os.path.abspath("./data/xyz")
# ---------- DB PASSWORD (hardcoded) ----------
DB_PASSWORD = "PutYourPasswordHere"  # or set to None and export from env var below
# ------------------------------------------------

if not DB_PASSWORD:
    raise SystemExit("DB_PASSWORD not set in script")

os.makedirs(LOCAL_DIR, exist_ok=True)

engine_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(engine_url, pool_pre_ping=True)

def export_table_to_local(table):
    sql = f"SELECT * FROM `{table}` LIMIT {LIMIT}"
    df = pd.read_sql_query(sql, engine)
    if df.empty:
        print(f"[WARN] {table} returned 0 rows")
        return
    out_path = os.path.join(LOCAL_DIR, f"{table}.parquet")
    df.to_parquet(out_path, engine="pyarrow", index=False)
    print(f"WROTE {len(df)} rows -> {out_path}")

if __name__ == "__main__":
    for t in TABLES:
        try:
            export_table_to_local(t)
        except Exception as e:
            print(f"[ERROR] {t}: {e}")
