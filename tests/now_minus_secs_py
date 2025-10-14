from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

def time_minus_secs(timezone: str, secs: int) -> str:
    """
    Return ISO 8601 string for (now in `timezone`) minus `secs` seconds.
    Requires Python 3.9+ (zoneinfo).
    """
    try:
        tz = ZoneInfo(timezone)
    except Exception as e:
        raise ValueError(f"Invalid timezone: {timezone}") from e

    now = datetime.now(tz)
    then = now - timedelta(seconds=int(secs))
    return then.isoformat()


