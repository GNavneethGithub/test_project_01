# framework/config_loader.py
import json
from pathlib import Path
from typing import Dict, Any

def load_project_config(path: str) -> Dict[str, Any]:
    """
    Load JSON config for a project.
    Expect keys: "api_", "aws", "logging".
    """
    cfg_file = Path(path)
    if not cfg_file.is_file():
        raise FileNotFoundError(f"Config file not found: {path}")

    with cfg_file.open("r", encoding="utf-8") as f:
        cfg = json.load(f)

    if "api_" not in cfg or "aws" not in cfg:
        raise ValueError("Config must contain 'api_' and 'aws' sections")

    return cfg
