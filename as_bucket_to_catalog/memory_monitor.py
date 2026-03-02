import os
import psutil
import pyarrow as pa
import time

PROCESS = psutil.Process(os.getpid())
START_TIME = time.perf_counter()
PEAK_RSS = 0


def get_rss_mb():
    return PROCESS.memory_info().rss / 1024 / 1024


def get_arrow_mb():
    return pa.total_allocated_bytes() / 1024 / 1024


def log_memory(stage: str):
    global PEAK_RSS

    rss = get_rss_mb()
    arrow_mem = get_arrow_mb()

    if rss > PEAK_RSS:
        PEAK_RSS = rss

    print(
        f"🧠 [{stage}] "
        f"RSS={rss:.2f}MB | "
        f"Arrow={arrow_mem:.2f}MB | "
        f"Peak={PEAK_RSS:.2f}MB"
    )