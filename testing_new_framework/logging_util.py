# logging_util.py
"""
Logging utility producing lines like:
LEVEL | [Thread] | logger func: message | UTC: 2025-10-12T09:24:26+00:00 | America/Los_Angeles: 2025-10-12T01:24:26-08:00 | Asia/Kolkata: 2025-10-12T14:54:26+05:30 | ctx={"user_message":"main","env":"dev","extra1":123}

Features:
- Three timezone stamps per line (default: UTC, America/Los_Angeles, Asia/Kolkata).
- Structured extras flattened into ctx JSON (supports arbitrary keys).
- One-time tzdata fallback warning when zoneinfo/tzdata not available.
- Thread-safe; suitable for multithread and multiprocessing Queue listener patterns.
- Convenience methods: debug_ctx, info_ctx, warning_ctx, error_ctx, critical_ctx.
"""

from __future__ import annotations
import logging
import sys
import json
import traceback
from typing import Optional, Dict, Any, Union, Iterable, List, Tuple
from datetime import datetime, timezone, timedelta
from logging.handlers import RotatingFileHandler

# Prefer zoneinfo; fall back if unavailable
try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except Exception:
    ZoneInfo = None

# Default TZ label/zone pairs shown in each log line
_DEFAULT_TZ_PAIRS: List[Tuple[str, str]] = [
    ("UTC", "UTC"),
    ("America/Los_Angeles", "America/Los_Angeles"),
    ("Asia/Kolkata", "Asia/Kolkata"),
]

# warn once when fallback is used
_warned_missing_tzdata = False


def _get_tzinfo_by_name(name: str):
    """
    Return tzinfo for zone name. Use ZoneInfo if present.
    If not available or zone not found return a fixed-offset tzinfo fallback.
    Emits one-time warning to stderr when falling back.
    """
    global _warned_missing_tzdata
    # try zoneinfo if available
    if ZoneInfo is not None:
        try:
            return ZoneInfo(name)
        except Exception:
            # fall through to fallback
            pass

    # fallback rules (no DST accuracy)
    n = name.upper()
    if n == "UTC":
        tz = timezone.utc
    elif "KOLKATA" in n or n == "IST" or n == "ASIA/KOLKATA":
        tz = timezone(timedelta(hours=5, minutes=30))
    elif "LOS_ANGELES" in n.replace("-", "_").upper() or "AMERICA/LOS_ANGELES" in n.upper() or n.startswith("PST"):
        # PST fallback -08:00 (ignores DST)
        tz = timezone(timedelta(hours=-8))
    else:
        tz = timezone.utc

    # one-time warning
    if ZoneInfo is None and not _warned_missing_tzdata:
        print(
            "WARNING: zoneinfo/tzdata not available; using fixed-offset tz fallbacks. "
            "Install tzdata for DST-aware zones (pip install tzdata).",
            file=sys.stderr,
        )
        _warned_missing_tzdata = True

    return tz


def _format_multi_timestamps(epoch_ts: float, tz_pairs: Iterable[Tuple[str, str]]) -> str:
    """
    Build a string "UTC:... | America/Los_Angeles:... | Asia/Kolkata:..."
    for the given epoch timestamp and label/zone pairs.
    """
    parts: List[str] = []
    for label, zone in tz_pairs:
        tzinfo = _get_tzinfo_by_name(zone)
        try:
            ts = datetime.fromtimestamp(epoch_ts, tz=tzinfo).isoformat(timespec="seconds")
        except Exception:
            ts = datetime.utcfromtimestamp(epoch_ts).replace(tzinfo=timezone.utc).isoformat(timespec="seconds")
        parts.append(f"{label}: {ts}")
    return " | ".join(parts)


class MultiTZFormatter(logging.Formatter):
    """
    Formatter injecting multi-zone timestamps into %(asctime)s and exposing %(ctx)s for JSON context.
    Final format:
    LEVEL | [Thread] | logger func: message | <multi-ts> | ctx=<json>
    """
    def __init__(self, tz_pairs: Optional[Iterable[Tuple[str, str]]] = None, fmt: Optional[str] = None):
        self.tz_pairs = list(tz_pairs or _DEFAULT_TZ_PAIRS)
        if fmt is None:
            fmt = "%(levelname)s | [%(threadName)s] | %(name)s %(funcName)s: %(message)s | %(asctime)s | ctx=%(ctx)s"
        super().__init__(fmt=fmt)

    def formatTime(self, record, datefmt=None):
        return _format_multi_timestamps(record.created, self.tz_pairs)


class StructuredAdapter(logging.LoggerAdapter):
    """
    LoggerAdapter that flattens arbitrary extra_data into ctx JSON and provides convenience methods.
    Methods:
      - log_with_ctx(level, msg, user_message=None, extra_data=None, stacklevel=3)
      - log_exception(msg, exc=None, user_message=None, extra_data=None, stacklevel=3)
      - debug_ctx/info_ctx/warning_ctx/error_ctx/critical_ctx
    """

    def process(self, msg, kwargs):
        # accept structured context via kwargs['extra'] or direct kwargs
        extra = kwargs.get("extra", {}) or {}
        user_message = extra.pop("user_message", None)
        extra_data = extra.pop("extra_data", None)

        # also accept direct kwargs
        user_message = kwargs.pop("user_message", user_message)
        extra_data = kwargs.pop("extra_data", extra_data)

        # flatten context: user_message plus keys from extra_data (if dict)
        ctx_obj: Dict[str, Any] = {}
        if user_message is not None:
            ctx_obj["user_message"] = user_message
        if extra_data is not None:
            if isinstance(extra_data, dict):
                # merge arbitrary keys into ctx
                for k, v in extra_data.items():
                    # avoid clobbering user_message if present
                    if k == "user_message":
                        continue
                    ctx_obj[k] = v
            else:
                ctx_obj["extra_data"] = extra_data

        # serialize ctx to JSON string for the %(ctx)s slot
        try:
            ctx_json = json.dumps(ctx_obj, default=str, ensure_ascii=False)
        except Exception:
            ctx_json = json.dumps({"ctx_serialize_error": True, "raw": str(ctx_obj)}, ensure_ascii=False)

        # attach ctx JSON into kwargs['extra'] so formatter uses it
        kwargs["extra"] = kwargs.get("extra", {})
        kwargs["extra"]["ctx"] = ctx_json
        return msg, kwargs

    def log_with_ctx(self, level: Union[int, str], msg: str, user_message: Optional[str] = None,
                     extra_data: Optional[Dict[str, Any]] = None, stacklevel: int = 3):
        """
        Primary logging call. level may be integer or string name.
        stacklevel defaults to 3 so funcName shows the caller of the adapter method.
        """
        if isinstance(level, str):
            level_val = logging.getLevelName(level.upper())
            if isinstance(level_val, str):
                # unknown string -> fallback to INFO numeric value
                level = logging.INFO
            else:
                level = level_val

        extra = {}
        if user_message is not None:
            extra["user_message"] = user_message
        if extra_data is not None:
            extra["extra_data"] = extra_data

        try:
            # prefer stacklevel so funcName points to true caller (Python 3.8+)
            self.log(level, msg, extra=extra, stacklevel=stacklevel)
        except TypeError:
            # older Python without stacklevel support
            self.log(level, msg, extra=extra)

    def log_exception(self, msg: str, exc: Optional[BaseException] = None,
                      user_message: Optional[str] = None, extra_data: Optional[Dict[str, Any]] = None,
                      stacklevel: int = 3):
        """Log exception with traceback included in ctx.traceback"""
        tb = traceback.format_exc() if exc is None else "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        extra = dict(extra_data or {})
        extra["traceback"] = tb
        self.log_with_ctx("ERROR", msg, user_message=user_message, extra_data=extra, stacklevel=stacklevel)

    # Convenience wrappers (adjust stacklevel so funcName points to caller)
    def debug_ctx(self, msg: str, user_message: Optional[str] = None, extra_data: Optional[Dict[str, Any]] = None, stacklevel: int = 2):
        self.log_with_ctx(logging.DEBUG, msg, user_message=user_message, extra_data=extra_data, stacklevel=stacklevel + 1)

    def info_ctx(self, msg: str, user_message: Optional[str] = None, extra_data: Optional[Dict[str, Any]] = None, stacklevel: int = 2):
        self.log_with_ctx(logging.INFO, msg, user_message=user_message, extra_data=extra_data, stacklevel=stacklevel + 1)

    def warning_ctx(self, msg: str, user_message: Optional[str] = None, extra_data: Optional[Dict[str, Any]] = None, stacklevel: int = 2):
        self.log_with_ctx(logging.WARNING, msg, user_message=user_message, extra_data=extra_data, stacklevel=stacklevel + 1)

    def error_ctx(self, msg: str, user_message: Optional[str] = None, extra_data: Optional[Dict[str, Any]] = None, stacklevel: int = 2):
        self.log_with_ctx(logging.ERROR, msg, user_message=user_message, extra_data=extra_data, stacklevel=stacklevel + 1)

    def critical_ctx(self, msg: str, user_message: Optional[str] = None, extra_data: Optional[Dict[str, Any]] = None, stacklevel: int = 2):
        self.log_with_ctx(logging.CRITICAL, msg, user_message=user_message, extra_data=extra_data, stacklevel=stacklevel + 1)


def get_logger(name: str = "pipeline_monitor",
               level: Union[int, str] = logging.INFO,
               tz_pairs: Optional[Iterable[Tuple[str, str]]] = None,
               log_to_stderr: bool = True,
               logfile: Optional[str] = None,
               rotate: bool = False,
               max_bytes: int = 10 * 1024 * 1024,
               backup_count: int = 5) -> StructuredAdapter:
    """
    Return a StructuredAdapter configured with the MultiTZFormatter.

    Args:
      name: logger name
      level: numeric level or string ("DEBUG","INFO",...)
      tz_pairs: iterable of (label, zone_name) tuples. Defaults to UTC, America/Los_Angeles, Asia/Kolkata
      logfile: optional path to file
      rotate: if True use RotatingFileHandler
    """
    # normalize level
    if isinstance(level, str):
        lvl = logging.getLevelName(level.upper())
        if isinstance(lvl, str):
            level = logging.INFO
        else:
            level = lvl

    logger = logging.getLogger(name)
    logger.setLevel(level)

    # idempotent handler setup
    if not logger.handlers:
        formatter = MultiTZFormatter(tz_pairs=tz_pairs)

        if log_to_stderr:
            sh = logging.StreamHandler(sys.stderr)
            sh.setFormatter(formatter)
            logger.addHandler(sh)

        if logfile:
            if rotate:
                fh = RotatingFileHandler(logfile, maxBytes=max_bytes, backupCount=backup_count)
            else:
                fh = logging.FileHandler(logfile)
            fh.setFormatter(formatter)
            logger.addHandler(fh)

    return StructuredAdapter(logger, {})


# Quick local debug when run as script
if __name__ == "__main__":
    lg = get_logger("test_logger", level="DEBUG", tz_pairs=None)
    lg.info_ctx("script start", user_message="main", extra_data={"env": "dev", "extra1": 123})
    def inner():
        lg.info_ctx("inner starting", user_message="inner-call", extra_data={"x": 1, "note": "detail"})
        try:
            1 / 0
        except Exception as e:
            lg.log_exception("error in inner", exc=e, user_message="inner-exc", extra_data={"x": 1})
    inner()
    lg.info_ctx("script end", user_message="main_end")
