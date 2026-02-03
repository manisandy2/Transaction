import requests
import time
import math
import logging
import argparse
import os
import sys

# ------------------ CONFIGURATION ------------------
DEFAULT_URL_PREFIX = "Transaction"
DEFAULT_BATCH_SIZE = 50000
DEFAULT_START_ROWS = 0
# DEFAULT_TOTAL_ROWS = 500000
DEFAULT_TOTAL_ROWS = 45000000
DEFAULT_MAX_RETRIES = 3
DEFAULT_SLEEP = 2

# ------------------ LOGGING SETUP ------------------
def setup_logging(url_prefix):
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)

    success_log_file = os.path.join(log_dir, f"success_{url_prefix}-live.log")
    failed_log_file = os.path.join(log_dir, f"error_{url_prefix}-live.log")

    # Success Logger
    success_logger = logging.getLogger("success_logger")
    success_logger.setLevel(logging.INFO)
    success_handler = logging.FileHandler(success_log_file)
    success_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    success_logger.addHandler(success_handler)

    # Failed Logger
    failed_logger = logging.getLogger("failed_logger")
    failed_logger.setLevel(logging.ERROR)
    failed_handler = logging.FileHandler(failed_log_file)
    failed_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    failed_logger.addHandler(failed_handler)
    
    # Console Logger (General)
    console_logger = logging.getLogger("console")
    console_logger.setLevel(logging.INFO)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter("%(message)s"))
    console_logger.addHandler(console_handler)

    return success_logger, failed_logger, console_logger

# ------------------ MAIN FUNCTION ------------------

def transfer_batches(args):
    api_url = f"{args.base_url}/{args.url_prefix}/ingest/mysql-range"
    success_logger, failed_logger, console = setup_logging(args.url_prefix)
    
    session = requests.Session()
    start = args.start
    total_rows = args.total
    batch_size = args.batch_size
    
    total_batches = math.ceil((total_rows - start) / batch_size)
    success_batches = 0
    failed_batches_count = 0
    batch_no = 1

    success_logger.info(f"Starting Data Transfer to {api_url}")
    success_logger.info(f"Range: {start}-{total_rows} | Batch Size: {batch_size} | Batches: {total_batches}")
    console.info(f"Starting Job: {total_batches} batches to process.")

    while start < total_rows:
        end = min(start + batch_size, total_rows)
        range_info = f"Rows {start:,}–{end:,}"
        console.info(f" Processing Batch {batch_no}/{total_batches} → {range_info}")

        batch_success = False

        for attempt in range(1, args.retries + 1):
            try:
                t0 = time.time()
                response = session.post(
                    api_url, 
                    params={"start_range": start, "end_range": end, "chunk_size": batch_size}, 
                    timeout=1800
                )

                if response.status_code == 200:
                    try:
                        result = response.json()
                        rows_written = result.get('rows_successful', 0) # align with new API response
                    except ValueError:
                        rows_written = "unknown"
                    
                    elapsed = round(time.time() - t0, 2)
                    
                    log_msg = f"Batch {batch_no} Success | {range_info} | Rows: {rows_written} | Time: {elapsed}s"
                    success_logger.info(log_msg)
                    console.info(f"  ✓ Completed in {elapsed}s")
                    
                    batch_success = True
                    success_batches += 1
                    break
                else:
                    console.warning(f"  ⚠ Failed (Attempt {attempt}) | HTTP {response.status_code}")
                    success_logger.warning(f"Batch {batch_no} Failed | {range_info} | Attempt {attempt} | HTTP {response.status_code}")
            
            except Exception as e:
                console.error(f"  ✖ Error (Attempt {attempt}): {str(e)}")
                failed_logger.error(f"Batch {batch_no} Error | {range_info} | Attempt {attempt} | Error: {str(e)}")
            
            time.sleep(5) # short sleep before retry

        if not batch_success:
            err_msg = f"Batch {batch_no} permanently failed after {args.retries} retries | {range_info}"
            failed_logger.error(err_msg)
            console.error(f"  ✖ Permanently failed. Skipping...")
            failed_batches_count += 1

        start += batch_size
        batch_no += 1
        time.sleep(args.sleep)

    summary = f"Transfer Completed | Success: {success_batches} | Failed: {failed_batches_count}"
    console.info("\n" + "="*30)
    console.info(summary)
    success_logger.info(summary)
    failed_logger.info(summary)

# ------------------ ARGS ------------------

def main():
    parser = argparse.ArgumentParser(description="Run Migration/Ingestion Job")
    parser.add_argument("--url-prefix", default=DEFAULT_URL_PREFIX, help="URL prefix for the endpoint")
    parser.add_argument("--base-url", default="http://127.0.0.1:8000", help="Base URL of the API")
    parser.add_argument("--start", type=int, default=DEFAULT_START_ROWS, help="Start row offset")
    parser.add_argument("--total", type=int, default=DEFAULT_TOTAL_ROWS, help="Total rows limit")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="Batch size")
    parser.add_argument("--retries", type=int, default=DEFAULT_MAX_RETRIES, help="Max retries per batch")
    parser.add_argument("--sleep", type=int, default=DEFAULT_SLEEP, help="Sleep seconds between batches")

    args = parser.parse_args()
    transfer_batches(args)

if __name__ == "__main__":
    main()