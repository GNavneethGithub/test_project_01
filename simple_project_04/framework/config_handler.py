import json
from pathlib import Path
from typing import Dict, Any

from airflow.models import Variable


def config_file_handler(abs_config_file_path: str) -> Dict[str, Any]:
    """
    Read a JSON configuration file and return its contents as a dict.
    """
    config_path = Path(abs_config_file_path)
    if not config_path.is_file():
        raise FileNotFoundError(f"Configuration file not found: {abs_config_file_path}")

    # read with explicit encoding
    with config_path.open("r", encoding="utf-8") as f:
        config_data = json.load(f)

    return config_data


def _safe_var_get(var_name: str, default: Any = None) -> Any:
    """
    Wrapper around Airflow Variable.get to provide a safe default.
    Use this so missing Airflow vars don't blow up.
    """
    try:
        return Variable.get(var_name, default_var=default)
    except Exception:
        # If Variable.get raises unexpectedly, return the default
        return default


def airflow_config_handler(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Fetch sensitive parameters from Airflow Variables and return a dict
    of grouped variables to merge into the file-based config.
    """
    # business_index_name is inside the elasticsearch section in your config file
    business_index_name = config.get("elasticsearch", {}).get("business_index_name")

    # general airflow variables
    airflow_variables = {
        "env": _safe_var_get("env")
    }

    # aws s3 related variables
    aws_s3_variables = {
        "aws_s3_bucket": _safe_var_get("aws_s3_bucket"),
        "aws_region": _safe_var_get("aws_region"),
        "aws_access_key_id": _safe_var_get("aws_access_key_id"),
        "aws_secret_access_key": _safe_var_get("aws_secret_access_key"),
    }

    # elasticsearch values are keyed by the business_index_name in your example
    if business_index_name:
        elastic_prefix = f"{business_index_name}_"
    else:
        # fallback prefix if business_index_name missing
        elastic_prefix = ""

    elasticsearch_variables = {
        "elasticsearch_host": _safe_var_get(f"{elastic_prefix}elasticsearch_host"),
        "elasticsearch_port": _safe_var_get(f"{elastic_prefix}elasticsearch_port"),
        "elasticsearch_user": _safe_var_get(f"{elastic_prefix}elasticsearch_user"),
        "elasticsearch_password": _safe_var_get(f"{elastic_prefix}elasticsearch_password"),
    }

    # snowflake defaults (fetch directly from Airflow variables, not from airflow_variables dict)
    snowflake_default_variables = {
        "snowflake_account": _safe_var_get("snowflake_account"),
        "snowflake_user": _safe_var_get("snowflake_user"),
        "snowflake_password": _safe_var_get("snowflake_password"),
        "snowflake_warehouse": _safe_var_get("snowflake_warehouse"),
        "snowflake_role": _safe_var_get("snowflake_role"),
    }

    all_variables = {
        "elasticsearch_variables": elasticsearch_variables,
        "aws_s3_variables": aws_s3_variables,
        "snowflake_default_variables": snowflake_default_variables,
        "env": airflow_variables["env"],
    }

    return all_variables


def main_config_handler(project_root_path: str, config_file_relative_path: str) -> Dict[str, Any]:
    """
    Combine file-based config with airflow variables and fill missing
    snowflake keys from defaults.
    """
    abs_config_file_path = str(Path(project_root_path) / config_file_relative_path)
    config = config_file_handler(abs_config_file_path)
    airflow_vars = airflow_config_handler(config)

    # Ensure snowflake section exists
    snowflake_cfg = config.setdefault("snowflake", {})
    default_vars = snowflake_cfg.setdefault("default_variables", {})

    # overlay snowflake defaults from Airflow (only where airflow provided values)
    # Only update keys that are not None (i.e. fetch succeeded)
    for k, v in airflow_vars.get("snowflake_default_variables", {}).items():
        if v is not None:
            # map Airflow keys to your default_variables schema if needed
            # If you want "snowflake_warehouse" in airflow to map to "warehouse" in config:
            mapped_key = k.replace("snowflake_", "")  # e.g. "snowflake_warehouse" -> "warehouse"
            default_vars[mapped_key] = v

    # Now fill missing keys on each snowflake sub-section from default_vars
    for section_name, section_values in snowflake_cfg.items():
        if section_name == "default_variables":
            continue
        # make sure the section is a dict
        if not isinstance(section_values, dict):
            continue
        for key, value in default_vars.items():
            # only set if key not present
            section_values.setdefault(key, value)

    # merge airflow vars into top-level config (intentional - confirm you want this)
    config.update(airflow_vars)

    return config





