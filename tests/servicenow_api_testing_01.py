# Simplest test: fetch any audit row (no filters)
import requests, json
INSTANCE = 'synopsys'
TOKEN = 'YOUR_BEARER_TOKEN'

url = f'https://{INSTANCE}.service-now.com/api/now/table/sys_audit'
params = {'sysparm_limit': 1}
headers = {'Accept': 'application/json', 'Authorization': f'Bearer {TOKEN}'}

r = requests.get(url, params=params, headers=headers, timeout=30)
print('URL:', r.request.url)
print('HTTP', r.status_code)
print(r.text)   # full raw body for debugging



# Check another table (confirm API and permissions) — if incident returns a record, API/auth is fine and problem is sys_audit-specific:
url2 = f'https://{INSTANCE}.service-now.com/api/now/table/incident'
r2 = requests.get(url2, params={'sysparm_limit':1}, headers=headers, timeout=30)
print('HTTP', r2.status_code)
print(r2.text)


# If you want to keep time filter, widen it and ensure UTC
from datetime import datetime, timedelta
now_utc = datetime.utcnow()
t0 = (now_utc - timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')   # last 7 days
query = f"sys_created_on>={t0}"
r3 = requests.get(f'https://{INSTANCE}.service-now.com/api/now/table/sys_audit',
                  params={'sysparm_query': query, 'sysparm_limit': 5},
                  headers=headers, timeout=30)
print('HTTP', r3.status_code)
print(r3.text)



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
