import time
from typing import Dict

def send_alert_if_runtime_exceeds(interval_minutes: float, max_total_minutes: float = 15.0) -> Dict:
    """
    Assumes caller did:
      from some_func import func_x
      from some_alert import alert_fn

    Behavior:
      1. Call func_x() immediately.
         - If False -> stop and return {"action":"completed","runs": n}.
         - If True  -> continue.
      2. While total elapsed < max_total_minutes:
         - sleep up to interval_minutes (but not past max_total_minutes)
         - after sleep call alert_fn(elapsed_seconds, runs)  # periodic alert
         - if elapsed >= max_total_minutes -> send final periodic alert then return {"action":"timeout","runs":n}
         - call func_x() again:
             - if False -> return {"action":"completed","runs":n}
             - if True  -> loop
      Exceptions from func_x() propagate. Exceptions from alert_fn() are ignored.
    """
    if interval_minutes <= 0:
        raise ValueError("interval_minutes must be > 0")
    if max_total_minutes <= 0:
        raise ValueError("max_total_minutes must be > 0")

    interval_s = interval_minutes * 60.0
    max_s = max_total_minutes * 60.0
    start = time.monotonic()
    runs = 0

    # first immediate call
    res = func_x()   # must be imported by caller; allow exceptions to bubble up
    runs += 1
    if res is False:
        return {"action": "completed", "runs": runs}
    if res is not True:
        raise TypeError("func_x() must return True or False")

    # periodic loop: sleep -> alert -> check timeout -> call func_x
    while True:
        elapsed = time.monotonic() - start
        remaining = max_s - elapsed
        if remaining <= 0:
            # final alert then timeout
            try:
                alert_fn(elapsed, runs)
            except Exception:
                pass
            return {"action": "timeout", "runs": runs}

        sleep_for = min(interval_s, remaining)
        time.sleep(sleep_for)

        elapsed = time.monotonic() - start
        # periodic alert
        try:
            alert_fn(elapsed, runs)
        except Exception:
            pass

        # if we've now crossed the max after the alert
        if elapsed >= max_s:
            return {"action": "timeout", "runs": runs}

        # call func_x again
        res = func_x()
        runs += 1
        if res is False:
            return {"action": "completed", "runs": runs}
        if res is not True:
            raise TypeError("func_x() must return True or False")
