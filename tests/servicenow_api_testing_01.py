import requests, json, time, argparse
from datetime import datetime, timedelta

# --------------- CONFIG ----------------
INSTANCE = "compnay_name"            # change to your instance name (no .service-now.com)
TOKEN = "YOUR_BEARER_TOKEN"      # change to your bearer token (or change script to use Basic Auth)
DEFAULT_TABLE = "sys_history_line"
PAGE_SIZE = 200
# ---------------------------------------

HEADERS = {
    "Accept": "application/json",
    "Authorization": f"Bearer {TOKEN}"
}

def parse_duration(s):
    """Parse duration strings like '30m', '1h', '2d' -> timedelta."""
    if s is None:
        return None
    s = s.strip().lower()
    if s.endswith('m'):
        return timedelta(minutes=int(s[:-1]))
    if s.endswith('h'):
        return timedelta(hours=int(s[:-1]))
    if s.endswith('d'):
        return timedelta(days=int(s[:-1]))
    # allow plain minutes
    try:
        return timedelta(minutes=int(s))
    except:
        raise ValueError("Duration must be like 30m, 1h, 2d or integer minutes")

def parse_dt(s):
    """Parse 'YYYY-MM-DD HH:MM:SS' in UTC -> datetime (naive, but treated as UTC)."""
    if s is None:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            pass
    raise ValueError("Datetime must be 'YYYY-MM-DD HH:MM:SS' (UTC) or ISO 'YYYY-MM-DDTHH:MM:SS'")

def build_query(start_dt, end_dt):
    """ServiceNow query for sys_created_on between start and end (inclusive)."""
    s = start_dt.strftime("%Y-%m-%d %H:%M:%S")
    e = end_dt.strftime("%Y-%m-%d %H:%M:%S")
    return f"sys_created_on>={s}^sys_created_on<={e}"

def fetch_window(instance, token, table, start_dt, end_dt, out_path, page_size=PAGE_SIZE):
    url = f"https://{instance}.service-now.com/api/now/table/{table}"
    headers = {"Accept": "application/json", "Authorization": f"Bearer {token}"}
    query = build_query(start_dt, end_dt)
    offset = 0
    total = 0
    with open(out_path, "w", encoding="utf-8") as fh:
        while True:
            params = {
                "sysparm_query": query,
                "sysparm_limit": page_size,
                "sysparm_offset": offset,
                "sysparm_display_value": "true",
                "sysparm_exclude_reference_link": "true"
                # omit sysparm_fields to get all fields for each row
            }
            r = requests.get(url, headers=headers, params=params, timeout=60)
            print(f"HTTP {r.status_code} offset={offset} limit={page_size}")
            if r.status_code == 200:
                payload = r.json()
                chunk = payload.get("result", []) or []
                if not chunk:
                    break
                for rec in chunk:
                    fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
                total += len(chunk)
                if len(chunk) < page_size:
                    break
                offset += page_size
                time.sleep(0.12)
            elif r.status_code in (401, 403):
                print("Auth/permission error:", r.status_code)
                print("Response (truncated):", r.text[:1000])
                raise SystemExit("Fix token/permissions and retry.")
            else:
                print("Unexpected status", r.status_code)
                print("Response (truncated):", r.text[:1000])
                raise SystemExit("Aborting due to unexpected response.")
    return total

# def main():
#     p = argparse.ArgumentParser(description="Fetch ServiceNow table rows for a time window and save NDJSON.")
#     p.add_argument("--table", default=DEFAULT_TABLE, help="Table to query (default sys_history_line)")
#     p.add_argument("--duration", help="Relative duration like 30m, 1h, 2d (used as 'last DURATION' when no start or with start to define window)")
#     p.add_argument("--start", help="UTC start datetime 'YYYY-MM-DD HH:MM:SS' (optional)")
#     p.add_argument("--end", help="UTC end datetime 'YYYY-MM-DD HH:MM:SS' (optional). If omitted and duration provided, end = start+duration or now.")
#     p.add_argument("--out", default="history_window.ndjson", help="Output NDJSON file path")
#     p.add_argument("--instance", default=INSTANCE, help="ServiceNow instance (no .service-now.com)")
#     p.add_argument("--token", default=TOKEN, help="Bearer token (will override TOKEN in script if provided)")
#     args = p.parse_args()

#     dur = parse_duration(args.duration) if args.duration else None
#     start = parse_dt(args.start) if args.start else None
#     end = parse_dt(args.end) if args.end else None

#     # compute window logic:
#     if start and end:
#         pass  # use as provided
#     elif start and dur:
#         end = start + dur
#     elif dur and not start:
#         end = datetime.utcnow()
#         start = end - dur
#     elif start and not end:
#         # use start -> now
#         end = datetime.utcnow()
#     else:
#         # default: past 1 hour
#         end = datetime.utcnow()
#         start = end - timedelta(hours=1)

#     # final sanity
#     if start >= end:
#         raise SystemExit("Start must be before end.")

#     print("Query window (UTC):", start.strftime("%Y-%m-%d %H:%M:%S"), "->", end.strftime("%Y-%m-%d %H:%M:%S"))
#     total = fetch_window(args.instance, args.token, args.table, start, end, args.out)
#     print(f"Done. Wrote {total} records to {args.out}")


def main(cli_args=None):
    """If cli_args is None: parse sys.argv (normal CLI run).
       If cli_args is a list: parse that list (programmatic call)."""
    p = argparse.ArgumentParser(description="Fetch ServiceNow table rows for a time window and save NDJSON.")
    p.add_argument("--table", default=DEFAULT_TABLE, help="Table to query (default sys_history_line)")
    p.add_argument("--duration", help="Relative duration like 30m, 1h, 2d")
    p.add_argument("--start", help="UTC start datetime 'YYYY-MM-DD HH:MM:SS'")
    p.add_argument("--end", help="UTC end datetime 'YYYY-MM-DD HH:MM:SS'")
    p.add_argument("--out", default="history_window.ndjson", help="Output NDJSON file path")
    p.add_argument("--instance", default=INSTANCE, help="ServiceNow instance (no .service-now.com)")
    p.add_argument("--token", default=TOKEN, help="Bearer token (overrides TOKEN in script if provided)")
    args = p.parse_args(cli_args)

    # Now reuse the same logic you already had to compute start/end and call fetch_window(...)
    dur = parse_duration(args.duration) if args.duration else None
    start = parse_dt(args.start) if args.start else None
    end = parse_dt(args.end) if args.end else None

    # compute window
    if start and end:
        pass
    elif start and dur:
        end = start + dur
    elif dur and not start:
        end = datetime.utcnow()
        start = end - dur
    elif start and not end:
        end = datetime.utcnow()
    else:
        end = datetime.utcnow()
        start = end - timedelta(hours=1)

    if start >= end:
        raise SystemExit("Start must be before end.")

    print("Query window (UTC):", start.strftime("%Y-%m-%d %H:%M:%S"), "->", end.strftime("%Y-%m-%d %H:%M:%S"))
    total = fetch_window(args.instance, args.token, args.table, start, end, args.out)
    print(f"Done. Wrote {total} records to {args.out}")

# -----------------------------
# Two ways to run from __main__:
# 1) Normal CLI run (click Run / Run File or run in terminal):
#    python fetch_history_ndjson.py --duration 30m --out out.ndjson --instance synopsys --token ABC
#
# 2) Programmatic: send the argument list from __main__ (this is what you wanted):
#    main(['--duration','30m','--out','out.ndjson','--instance','synopsys','--token','ABC'])
# -----------------------------

# if __name__ == "__main__":
#     # Option A: simply use CLI / VS Code Run button (parses sys.argv)
#     main()

#     # Option B: pass args programmatically (uncomment to use)
#     main(['--duration', '30m', '--out', 'history_last_30m.ndjson',
#           '--instance', 'synopsys', '--token', 'YOUR_BEARER_TOKEN'])


if __name__ == "__main__":
    start_ts = '2025-09-17 09:30:00'
    end_ts = '2025-09-17 10:00:00'
    start_ts_str = start_ts.replace(' ', '_').replace(':', '-')
    end_ts_str = end_ts.replace(' ', '_').replace(':', '-')
    main([
      '--start', start_ts,
      '--end',   end_ts,
      '--out',  '{DEFAULT_TABLE}_{start_ts_str}_{end_ts_str}.ndjson',
      '--instance','synopsys',
      '--token','YOUR_TOKEN'
    ])


















# import requests, json, pprint
# INST = "compnay_name"
# TOKEN = "YOUR_BEARER_TOKEN"
# HEADERS = {"Accept":"application/json","Authorization":f"Bearer {TOKEN}"}

# url = f"https://{INST}.service-now.com/api/now/table/sys_history_line"
# params = {
#   "sysparm_limit": 10,
#   "sysparm_fields": "sys_id,sys_created_on,user_name,user_id,field,label,old,old_value,new,new_value,type,relation,update_time"
# }
# r = requests.get(url, headers=HEADERS, params=params, timeout=30)
# print("HTTP", r.status_code)
# pprint.pprint(r.json())


# # probe_history_audit_tables.py
# import requests, json, pprint, time

# INSTANCE = "compnay_name"          # no .service-now.com
# TOKEN = "YOUR_BEARER_TOKEN"

# HEADERS = {"Accept": "application/json", "Authorization": f"Bearer {TOKEN}"}
# CANDIDATES = [
#     "sys_history_set",
#     "sys_history_line",
#     "sys_journal_field",
#     "sys_audit",
#     "sys_audit_relation",
#     "sys_audit_role",
#     "sys_upgrade_history",
#     "sys_scheduler_job_history"
# ]

# def probe(table):
#     url = f"https://{INSTANCE}.service-now.com/api/now/table/{table}"
#     params = {"sysparm_limit": 1}
#     r = requests.get(url, headers=HEADERS, params=params, timeout=30)
#     return r.status_code, r.text

# if __name__ == "__main__":
#     for t in CANDIDATES:
#         status, body = probe(t)
#         print(f"{t} -> HTTP {status}")
#         try:
#             j = json.loads(body)
#         except Exception:
#             print("  (non-JSON response, truncated):", body[:400])
#             continue
#         res = j.get("result", [])
#         if res:
#             print("  >>> non-empty. Sample record from", t)
#             pprint.pprint(res[0])
#             break
#         else:
#             print("  empty result")
#         time.sleep(0.15)
#     else:
#         print("No candidate returned rows. Likely: auditing disabled, retention cleaned, or ACLs hide rows.")







# # save as list_sys_tables.py
# import requests

# INSTANCE = "compnay_name"               # change to your instance (no .service-now.com)
# TOKEN = "YOUR_BEARER_TOKEN"         # change to your token or use Basic Auth instead

# URL = f"https://{INSTANCE}.service-now.com/api/now/table/sys_db_object"
# HEADERS = {"Accept": "application/json", "Authorization": f"Bearer {TOKEN}"}

# def list_sys_tables(page_size=200):
#     offset = 0
#     all_rows = []
#     while True:
#         params = {
#             "sysparm_query": "nameLIKEsys",           # finds table names containing "sys"
#             "sysparm_fields": "name,label",
#             "sysparm_limit": page_size,
#             "sysparm_offset": offset
#         }
#         r = requests.get(URL, headers=HEADERS, params=params, timeout=30)
#         print(f"HTTP {r.status_code} -> offset={offset} limit={page_size}")
#         if r.status_code != 200:
#             print("ERROR body:", r.text)
#             return None

#         data = r.json().get("result", [])
#         if not data:
#             break
#         all_rows.extend(data)
#         if len(data) < page_size:
#             break
#         offset += page_size

#     return all_rows

# if __name__ == "__main__":
#     rows = list_sys_tables()
#     if rows is None:
#         raise SystemExit("Failed to fetch table list — check token/permissions.")
#     print(f"Found {len(rows)} tables containing 'sys':")
#     for r in rows:
#         print(r.get("name"), "-", r.get("label"))





# # Simplest test: fetch any audit row (no filters)
# import requests, json
# INSTANCE = 'compnay_name'
# TOKEN = 'YOUR_BEARER_TOKEN'

# url = f'https://{INSTANCE}.service-now.com/api/now/table/xyz'
# params = {'sysparm_limit': 1}
# headers = {'Accept': 'application/json', 'Authorization': f'Bearer {TOKEN}'}

# r = requests.get(url, params=params, headers=headers, timeout=30)
# print('URL:', r.request.url)
# print('HTTP', r.status_code)
# print(r.text)   # full raw body for debugging



# # Check another table (confirm API and permissions) — if incident returns a record, API/auth is fine and problem is xyz-specific:
# url2 = f'https://{INSTANCE}.service-now.com/api/now/table/incident'
# r2 = requests.get(url2, params={'sysparm_limit':1}, headers=headers, timeout=30)
# print('HTTP', r2.status_code)
# print(r2.text)


# # If you want to keep time filter, widen it and ensure UTC
# from datetime import datetime, timedelta
# now_utc = datetime.utcnow()
# t0 = (now_utc - timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')   # last 7 days
# query = f"sys_created_on>={t0}"
# r3 = requests.get(f'https://{INSTANCE}.service-now.com/api/now/table/xyz',
#                   params={'sysparm_query': query, 'sysparm_limit': 5},
#                   headers=headers, timeout=30)
# print('HTTP', r3.status_code)
# print(r3.text)



# # Save as get_one_audit_record.py
# import requests
# from pprint import pprint

# # --- Fill/confirm these (your sample gave these already) ---
# INSTANCE = 'company_name'   # will call https://company_name.service-now.com
# BEARER_TOKEN = 'xyzabscerDFGnkfpbvo'

# # --- API call: get the latest aaaaaaaaaaaaaaaaaaaa record (limit=1) ---
# base_url = f'https://{INSTANCE}.service-now.com/api/now/table/aaaaaaaaaaaaaaaaaaaa'
# params = {
#     'sysparm_query': 'ORDERBYDESCsys_created_on',   # sort newest first
#     'sysparm_limit': 1,
#     'sysparm_fields': 'sys_id,sys_created_on,documentkey,field,old_value,new_value'  # change fields as needed
# }
# headers = {
#     'Accept': 'application/json',
#     'Authorization': f'Bearer {BEARER_TOKEN}'
# }

# try:
#     resp = requests.get(base_url, params=params, headers=headers, timeout=30)
# except requests.RequestException as e:
#     raise SystemExit(f"Request failed: {e}")

# print(f"HTTP {resp.status_code}")
# if resp.status_code != 200:
#     print("Response body (first 1000 chars):")
#     print(resp.text[:1000])
#     raise SystemExit("API request failed — check token, instance, and permissions.")

# payload = resp.json()
# # ServiceNow returns results in payload['result']
# records = payload.get('result') or []
# if not records:
#     print("No records returned (empty result).")
# else:
#     print("One sample record (pretty):")
#     pprint(records[0])
