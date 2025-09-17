# Save as get_one_audit_record.py
import requests
from pprint import pprint

# --- Fill/confirm these (your sample gave these already) ---
INSTANCE = 'company_name'   # will call https://company_name.service-now.com
BEARER_TOKEN = 'xyzabscerDFGnkfpbvo'

# --- API call: get the latest aaaaaaaaaaaaaaaaaaaa record (limit=1) ---
base_url = f'https://{INSTANCE}.service-now.com/api/now/table/aaaaaaaaaaaaaaaaaaaa'
params = {
    'sysparm_query': 'ORDERBYDESCsys_created_on',   # sort newest first
    'sysparm_limit': 1,
    'sysparm_fields': 'sys_id,sys_created_on,documentkey,field,old_value,new_value'  # change fields as needed
}
headers = {
    'Accept': 'application/json',
    'Authorization': f'Bearer {BEARER_TOKEN}'
}

try:
    resp = requests.get(base_url, params=params, headers=headers, timeout=30)
except requests.RequestException as e:
    raise SystemExit(f"Request failed: {e}")

print(f"HTTP {resp.status_code}")
if resp.status_code != 200:
    print("Response body (first 1000 chars):")
    print(resp.text[:1000])
    raise SystemExit("API request failed — check token, instance, and permissions.")

payload = resp.json()
# ServiceNow returns results in payload['result']
records = payload.get('result') or []
if not records:
    print("No records returned (empty result).")
else:
    print("One sample record (pretty):")
    pprint(records[0])
