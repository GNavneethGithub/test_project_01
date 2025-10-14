from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple
from zoneinfo import ZoneInfo  # Python 3.9+
import sys

def _parse_iso_to_dt(iso: str, assumed_tz: Optional[str] = None) -> datetime:
    """
    Parse ISO-8601 string to an aware datetime.
    - accepts trailing 'Z' or an offset like '+05:30'.
    - if naive and assumed_tz provided, attach that zone.
    - raises ValueError if naive and no assumed_tz.
    """
    if iso.endswith("Z"):
        iso = iso[:-1] + "+00:00"
    dt = datetime.fromisoformat(iso)
    if dt.tzinfo is None:
        if not assumed_tz:
            raise ValueError("timestamp has no offset; provide assumed_tz_for_naive")
        dt = dt.replace(tzinfo=ZoneInfo(assumed_tz))
    return dt

def now_minus_iso(
    iso_ts: str,
    *,
    assumed_tz_for_naive: Optional[str] = None,
    result_tz: Optional[str] = None
) -> Tuple[timedelta, int]:
    """
    Return (timedelta, seconds) for (now - iso_ts).

    Parameters
    - iso_ts: ISO-8601 timestamp string (e.g. "2025-10-14T14:33:21+05:30" or "2025-10-14T08:00:00" if assumed_tz_for_naive provided)
    - assumed_tz_for_naive: timezone name (e.g. "Asia/Kolkata") to interpret naive timestamps
    - result_tz: if provided (e.g. "Asia/Kolkata"), compute both now and iso_ts in this zone before subtracting. If None uses UTC.

    Returns
    - (timedelta, seconds) where seconds is int(total_seconds()).
    """
    dt = _parse_iso_to_dt(iso_ts, assumed_tz_for_naive)
    target_tz = timezone.utc if result_tz is None else ZoneInfo(result_tz)

    now = datetime.now(target_tz)
    dt_in_target = dt.astimezone(target_tz)

    td = now - dt_in_target
    secs = int(td.total_seconds())
    return td, secs

def diff_between_iso(
    st1: str,
    st2: str,
    *,
    assumed_tz_for_naive: Optional[str] = None,
    result_tz: Optional[str] = None
) -> Tuple[timedelta, int]:
    """
    Return (timedelta, seconds) for (st1 - st2).
    - st1, st2: ISO-8601 strings (must include offset or be naive and have assumed_tz_for_naive).
    - assumed_tz_for_naive: timezone name like 'Asia/Kolkata' to interpret naive inputs.
    - result_tz: if provided, convert both datetimes to this zone before subtracting (else UTC).
    """
    dt1 = _parse_iso_to_dt(st1, assumed_tz_for_naive)
    dt2 = _parse_iso_to_dt(st2, assumed_tz_for_naive)

    target = timezone.utc if result_tz is None else ZoneInfo(result_tz)
    td = dt1.astimezone(target) - dt2.astimezone(target)
    secs = int(td.total_seconds())
    return td, secs


tz = ZoneInfo("Asia/Kolkata")         # or ZoneInfo("America/Los_Angeles")
now = datetime.now(tz)                # timezone-aware datetime

iso_full = now.isoformat()             # includes offset and microseconds
iso_seconds = now.isoformat(timespec="seconds")  # seconds precision, e.g. "2025-10-14T14:33:21+05:30"

print(iso_full)
print(iso_seconds)


if __name__ == "__main__":
    a = "2025-10-14T14:33:21+05:30"
    b = "2025-10-14T14:30:00+07:30"
    td, secs = diff_between_iso(a, b)
    print("timedelta:", td)
    print("seconds:", secs)

    # example using named result_tz (requires tzdata on some platforms, e.g. Windows)
    try:
        td2, secs2 = diff_between_iso(a, b, result_tz="Asia/Kolkata")
        print("timedelta (Asia/Kolkata):", td2, "seconds:", secs2)
    except Exception as exc:
        print("named tz failed:", exc, file=sys.stderr)
