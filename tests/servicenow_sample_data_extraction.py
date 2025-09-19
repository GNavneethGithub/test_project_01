# sn_export.py
#!/usr/bin/env python3
import json, os, sys, time, logging, urllib.parse, requests
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Tuple

# ---------------- Logging ----------------
log = logging.getLogger("sn_export")
def setup_logging(level="INFO", log_file: Optional[str] = None):
    lvl = getattr(logging, str(level).upper(), logging.INFO)
    fmt = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    datefmt = "%Y-%m-%dT%H:%M:%S%z"
    handlers = [logging.StreamHandler(sys.stdout)]
    if log_file: handlers.append(logging.FileHandler(log_file, encoding="utf-8"))
    logging.basicConfig(level=lvl, format=fmt, datefmt=datefmt, handlers=handlers)

# ---------------- Utils ----------------
def _parse_dt(s: str) -> Optional[datetime]:
    for fmt in ("%Y-%m-%d %H:%M:%S","%Y-%m-%dT%H:%M:%S","%Y-%m-%d"):
        try: return datetime.strptime(s, fmt)
        except ValueError: pass
    try: return datetime.fromisoformat(s.replace("Z","+00:00"))
    except ValueError: return None

def _to_glide_ts(s: str) -> str:
    dt = _parse_dt(s)
    if dt is None: 
        log.warning("Unrecognized ts '%s' -> passing as-is to query", s); return s
    if dt.tzinfo: dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def _to_token_ts(s: str) -> str:
    dt = _parse_dt(s)
    if dt:
        if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
        else: dt = dt.astimezone(timezone.utc)
        return dt.strftime("%Y%m%dT%H%M%SZ")
    return ("".join(ch if ch.isalnum() else "_" for ch in s))[:64]

def _api_url(instance: str, table: str) -> str:
    return f"https://{instance}.service-now.com/api/now/table/{table}"

def _headers(token: str) -> Dict[str,str]:
    return {"Accept":"application/json","Authorization":f"Bearer {token}"}

def _encode_query(clauses: List[str]) -> str:
    return urllib.parse.quote("^".join([c for c in clauses if c]), safe=":^<>=@ _%-")

def _write_ndjson(path: str, records: Iterable[Dict]) -> int:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    n = 0
    with open(path,"w",encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False)+"\n"); n += 1
    return n

# ---------------- Public API ----------------
@dataclass
class ExportResult:
    table: str
    start_ts: str
    end_ts: str
    out_path: str
    rows: int
    started_epoch: int
    finished_epoch: int

def export_table(
    *,
    instance: str,
    token: str,
    table: str,
    start_ts: str,
    end_ts: str,
    out_dir: str = "./out",
    fields: Optional[List[str]] = None,
    extra_query: Optional[str] = None,
    time_field: str = "sys_created_on",   # change to 'sys_updated_on' for snapshots
    display_value: str = "all",           # 'true' | 'false' | 'all'
    batch_size: int = 5000,
    max_pages: Optional[int] = None,
    timeout: float = 60.0,
    retries: int = 5,
    backoff_initial: float = 1.0,
    log_level: str = "INFO",
    log_file: Optional[str] = None,
) -> ExportResult:
    """
    Programmatic export. Returns ExportResult and writes NDJSON-lines to a '.sjon' file.
    """
    setup_logging(log_level, log_file)
    started = int(time.time())

    # Prepare filename
    start_q = _to_glide_ts(start_ts)
    end_q   = _to_glide_ts(end_ts)
    start_tok = _to_token_ts(start_ts)
    end_tok   = _to_token_ts(end_ts)
    now_tok   = str(int(time.time()))
    fname = f"{table}__{start_tok}__{end_tok}__{now_tok}.sjon"
    out_path = os.path.join(out_dir, fname)

    # Build Glide query
    clauses = [f"{time_field}>={start_q}", f"{time_field}<={end_q}"]
    if extra_query: clauses.append(extra_query.strip())
    sysparm_query = _encode_query(clauses)

    url = _api_url(instance, table)
    session = requests.Session(); session.headers.update(_headers(token))

    def _fetch() -> Iterable[Dict]:
        offset, page, backoff = 0, 0, backoff_initial
        while True:
            if max_pages is not None and page >= max_pages:
                log.info("Reached max_pages=%s; stop.", max_pages); break
            params = {
                "sysparm_query": sysparm_query,
                "sysparm_limit": str(batch_size),
                "sysparm_offset": str(offset),
                "sysparm_display_value": display_value,
            }
            if fields: params["sysparm_fields"] = ",".join(fields)
            full_url = f"{url}?{urllib.parse.urlencode(params)}"
            attempt = 0
            while True:
                attempt += 1
                try:
                    resp = session.get(full_url, timeout=timeout)
                    if resp.status_code in (429,500,502,503,504):
                        ra = resp.headers.get("Retry-After")
                        sleep_for = float(ra) if (ra and ra.isdigit()) else backoff
                        log.warning("HTTP %s -> backoff %.2fs (attempt %d/%d)",
                                    resp.status_code, sleep_for, attempt, retries)
                        time.sleep(sleep_for)
                        backoff = min(backoff*2, 60.0)
                        if attempt < retries: continue
                    resp.raise_for_status(); break
                except requests.RequestException as e:
                    log.error("Request error (attempt %d/%d): %s", attempt, retries, e)
                    if attempt >= retries: raise
                    time.sleep(backoff); backoff = min(backoff*2, 60.0)

            data = resp.json()
            rows = data.get("result") or []
            if not rows:
                log.info("No more rows (page %d).", page); break
            for r in rows: yield r
            got = len(rows)
            offset += got; page += 1; backoff = backoff_initial
            if got < batch_size:
                log.info("Last page (got=%d < limit=%d).", got, batch_size); break

    try:
        rows = _write_ndjson(out_path, _fetch())
    except Exception as e:
        log.exception("Export failed: %s", e); raise

    finished = int(time.time())
    log.info("Export OK: table=%s rows=%d file=%s", table, rows, out_path)
    return ExportResult(
        table=table, start_ts=start_ts, end_ts=end_ts,
        out_path=out_path, rows=rows,
        started_epoch=started, finished_epoch=finished
    )

# ---------------- Optional CLI wrapper ----------------
def _parse_cli():
    import argparse
    p = argparse.ArgumentParser(description="ServiceNow export (NDJSON lines to .sjon)")
    p.add_argument("--instance", required=True)
    p.add_argument("--token", required=True)
    p.add_argument("--table", required=True)
    p.add_argument("--start-ts", required=True)
    p.add_argument("--end-ts", required=True)
    p.add_argument("--out-dir", default="./out")
    p.add_argument("--fields", default=None, help="comma list")
    p.add_argument("--extra-query", default=None)
    p.add_argument("--time-field", default="sys_created_on")
    p.add_argument("--display-value", default="all", choices=["true","false","all"])
    p.add_argument("--batch-size", type=int, default=5000)
    p.add_argument("--max-pages", type=int, default=None)
    p.add_argument("--timeout", type=float, default=60.0)
    p.add_argument("--retries", type=int, default=5)
    p.add_argument("--backoff-initial", type=float, default=1.0)
    p.add_argument("--log-level", default="INFO")
    p.add_argument("--log-file", default=None)
    return p.parse_args()

def main():
    args = _parse_cli()
    fields_list = [f.strip() for f in args.fields.split(",")] if args.fields else None
    export_table(
        instance=args.instance, token=args.token, table=args.table,
        start_ts=args.start_ts, end_ts=args.end_ts, out_dir=args.out_dir,
        fields=fields_list, extra_query=args.extra_query, time_field=args.time_field,
        display_value=args.display_value, batch_size=args.batch_size,
        max_pages=args.max_pages, timeout=args.timeout, retries=args.retries,
        backoff_initial=args.backoff_initial, log_level=args.log_level, log_file=args.log_file
    )

# if __name__ == "__main__":
#     main()

# # runner.py
# from sn_export import export_table

res = export_table(
    instance="your_instance",
    token="YOUR_BEARER_TOKEN",
    table="sys_audit",
    start_ts="2025-09-18 00:00:00",
    end_ts="2025-09-19 00:00:00",
    out_dir="./out",
    extra_query="tablename=incident",     # optional
    fields=None,                          # or ["sys_id","sys_created_on","fieldname","oldvalue","newvalue"]
    time_field="sys_created_on",          # or "sys_updated_on" for snapshots
    display_value="all",
    log_level="DEBUG"
)

print("Wrote:", res.out_path, "rows:", res.rows)




