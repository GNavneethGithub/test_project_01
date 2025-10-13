# simple_per_process.py
from multiprocessing import Process
import time
import random
from logging_util import get_logger

def sub_function(task_id:int):
    # create logger inside process to ensure handlers exist in that process
    logger = get_logger(f"worker_{task_id}", level="INFO", tz_pairs=None)
    corr = f"{task_id}"
    logger.log_with_ctx("INFO", "start sub_function", user_message="sub_start", extra_data={"task_id":task_id, "corr":corr})
    time.sleep(random.uniform(0.2, 1.0))
    logger.log_with_ctx("INFO", "end sub_function", user_message="sub_end", extra_data={"task_id":task_id})

def main(n=4):
    procs=[]
    for i in range(n):
        p = Process(target=sub_function, args=(i,), name=f"proc-{i}")
        p.start()
        procs.append(p)
    for p in procs:
        p.join()

if __name__=="__main__":
    main(6)
