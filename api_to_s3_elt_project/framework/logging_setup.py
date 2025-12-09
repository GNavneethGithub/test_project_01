# framework/logging_setup.py
import logging
from typing import Dict, Any

def setup_logging(config: Dict[str, Any]) -> logging.Logger:
    """
    Configure root logger based on config["logging"]["level"].
    """
    level_name = config.get("logging", {}).get("level", "INFO")
    level = getattr(logging, str(level_name).upper(), logging.INFO)

    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )
    return logging.getLogger("rapid7_framework")
