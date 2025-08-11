# run_in_k8_via_ssh.py
import json, re
from airflow.providers.ssh.hooks.ssh import SSHHook

SSH_CONN_ID = "ssh_k8"                 # provided by your team
NAMESPACE   = "default"                # or your real namespace
POD_NAME    = "my-pod-0"
SCRIPT_PATH = "/opt/tools/run_sub_sub.py"

def run_in_k8(config, record):
    payload = json.dumps({"config": config, "record": record})
    ssh_hook = SSHHook(ssh_conn_id=SSH_CONN_ID)
    with ssh_hook.get_conn() as ssh:
        cmd = f"kubectl -n {NAMESPACE} exec -i {POD_NAME} -- python {SCRIPT_PATH} --stdin"
        stdin, stdout, stderr = ssh.exec_command(cmd)
        stdin.write(payload)
        stdin.flush()
        stdin.channel.shutdown_write()

        pod_stdout = stdout.read().decode()
        pod_stderr = stderr.read().decode()
        if pod_stderr:
            print(pod_stderr)  # shows in Airflow logs

        m = re.search(r"BEGIN_JSON_RESULT\s*(\{.*?\})\s*END_JSON_RESULT", pod_stdout, re.S)
        if not m:
            raise RuntimeError(f"No JSON result found in output:\n{pod_stdout}")
        out = json.loads(m.group(1))
        if not out.get("ok", False):
            raise RuntimeError(f"Pod reported error: {out}")
        return out["result"]
