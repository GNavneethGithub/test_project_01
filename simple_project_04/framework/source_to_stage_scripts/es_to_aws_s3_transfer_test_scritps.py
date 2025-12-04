import subprocess
import logging
import traceback
from typing import List, Dict, Any

logger = logging.getLogger(__name__)




import re
import json
import traceback
from typing import Dict, Any, List, Optional

import subprocess



def redact_secrets(text: str) -> str:
    if not text:
        return text
    text = re.sub(r'(--s3AccessKeyId=)\S+', r'\1***REDACTED***', text)
    text = re.sub(r'(--s3SecretAccessKey=)\S+', r'\1***REDACTED***', text)
    text = re.sub(r'(--headers=)\S+', r'\1***REDACTED***', text)
    text = re.sub(r'AKIA[0-9A-Z]{16}', '***REDACTED_AKIA***', text)
    return text



def test_run_elasticdump_command(
    final_cmd_list: List[str],
    timeout_seconds: int = 600
) -> Dict[str, Any]:
    """
    Minimal test function:
    - runs the command
    - redacts secrets
    - logs stdout & stderr (Airflow UI will show this)
    - does NOT parse records/files
    - returns raw data for inspection
    """

    result = {
        "returncode": None,
        "stdout": "",
        "stderr": "",
        "error_message": None,
    }

    logger.info("=== Running elasticdump test command ===")
    logger.info(f"Command (redacted): {redact_secrets(' '.join(final_cmd_list))}")

    try:
        proc = subprocess.run(
            final_cmd_list,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            check=False
        )

        # redact output
        red_stdout = redact_secrets(proc.stdout or "")
        red_stderr = redact_secrets(proc.stderr or "")

        # log to Airflow UI
        logger.info("=== STDOUT ===")
        logger.info(red_stdout)

        if red_stderr.strip():
            logger.warning("=== STDERR ===")
            logger.warning(red_stderr)

        result["returncode"] = proc.returncode
        result["stdout"] = red_stdout
        result["stderr"] = red_stderr

        logger.info(f"=== Return code: {proc.returncode} ===")

        return result

    except subprocess.TimeoutExpired as e:
        logger.error(f"Command timed out after {timeout_seconds} seconds")
        result["error_message"] = f"TimeoutExpired: {str(e)}"
        return result

    except Exception as e:
        tb = traceback.format_exc()
        err = f"Unexpected error while running command: {str(e)}\n{tb}"
        logger.error(err)
        result["error_message"] = err
        return result
