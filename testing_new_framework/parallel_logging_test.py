# nested_parallel_test.py
"""
Parallel nested logging test.
Run: python nested_parallel_test.py
Requires logging_util.py in the same folder.
"""

import threading
import time
import uuid
import random

from logging_util import get_logger

logger = get_logger("parallel_nested_test", level="DEBUG")  # prints all levels

def func_1(correlation_id):
    logger.info_ctx("inside func_1", user_message="f1", extra_data={"correlation_id": correlation_id})
    time.sleep(0.2)

def func_2(correlation_id):
    # introduce occasional error to exercise exception logging
    logger.info_ctx("inside func_2", user_message="f2", extra_data={"correlation_id": correlation_id})
    time.sleep(0.1)
    if random.random() < 0.2:
        raise RuntimeError("simulated error in func_2")

def sub_function(task_id):
    corr = f"{task_id}-{uuid.uuid4().hex[:8]}"
    logger.info_ctx("sub_function start", user_message="sub_start", extra_data={"correlation_id": corr, "task_id": task_id})
    try:
        func_1(corr)
        func_2(corr)
    except Exception as exc:
        # include correlation id and task_id in exception context
        logger.log_exception("exception in sub_function", exc=exc,
                             user_message="sub_exception",
                             extra_data={"correlation_id": corr, "task_id": task_id})
    else:
        logger.info_ctx("sub_function end", user_message="sub_end", extra_data={"correlation_id": corr, "task_id": task_id})

def worker(task_id):
    sub_function(task_id)

def main(thread_count: int = 6):
    logger.info_ctx("main start", user_message="main_start", extra_data={"thread_count": thread_count})
    threads = []
    for i in range(thread_count):
        t = threading.Thread(target=worker, args=(i,), name=f"worker-{i}")
        threads.append(t)
        t.start()
    for t in threads:
        t.join()
    logger.info_ctx("main end", user_message="main_end", extra_data={"thread_count": thread_count})

if __name__ == "__main__":
    random.seed(42)
    main(thread_count=6)
