# from fastapi import APIRouter, Query, Body, HTTPException
import time
# from typing import Optional, List, Dict, Any
# Fixed imports
# from utility import schema-data
from bucket_to_catalog.table_utility import clean_rows, process_chunk
from bucket_to_catalog.transaction_utility import *
from core.catalog_client import get_catalog_client
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyiceberg.catalog import NoSuchTableError
from core.logger import get_logger
from bucket_to_catalog.error_handler import handle_ingestion_error
from bucket_to_catalog.read_bucket import list_json_files,iter_json_files,update_last_pri_id,get_last_value,get_last_column_name
from core.r2_client import get_r2_client
from routers.bucket_to_r2Catalog import *
import json
import psutil
import os
import gc


logger = get_logger(__name__)

def process_batch(executor, batch_keys, bucket, table, arrow_schema, current_pri_id):

    batch_rows = []
    seen_batch_ids = set()

    futures = [
        executor.submit(
            get_r2_client().get_object,
            Bucket=bucket,
            Key=key
        )
        for key in batch_keys
    ]

    for future in as_completed(futures):
        try:
            obj = future.result()
            raw_data = json.loads(obj["Body"].read())
            rows = extract_rows_from_json(raw_data)

            for row in rows:
                uuid_val = row.get("pri_id")
                if not uuid_val or uuid_val in seen_batch_ids:
                    continue

                current_pri_id += 1
                row["pri_id_backup"] = current_pri_id
                row["pri_id"] = uuid_val
                # print(row["pri_id_backup"])
                batch_rows.append(row)
                seen_batch_ids.add(uuid_val)

        except Exception as e:
            print(f"File processing error: {e}")

    if not batch_rows:
        return 0

    cleaned = clean_rows(
        rows=batch_rows,
        boolean_fields=BOOLEAN_FIELDS,
        timestamps_fields=TIMESTAMP_FIELDS,
        field_overrides=FIELD_OVERRIDES
    )

    arrow_table, errors = process_chunk(cleaned, arrow_schema)

    if arrow_table and arrow_table.num_rows > 0:
        table.append(arrow_table)
        inserted = arrow_table.num_rows
    else:
        inserted = 0

    del batch_rows, cleaned, arrow_table
    return inserted

def print_memory(stage=""):
    process = psutil.Process(os.getpid())
    mem_info = process.memory_info()

    rss_mb = mem_info.rss / (1024 * 1024)   # actual memory used
    # vms_mb = mem_info.vms / (1024 * 1024)   # virtual memory

    # print(f"🧠 Memory [{stage}] -> RSS: {rss_mb:.2f} MB | VMS: {vms_mb:.2f} MB")
    print(f"🧠 Memory [{stage}] -> RSS: {rss_mb:.2f} MB")


def extract_rows_from_json(raw_data):
    """
    Convert different JSON formats into list[dict]
    """
    # print("start process")
    # Case 1: Already list of rows
    if isinstance(raw_data, list):
        # print("list")
        return raw_data

    # Case 2: {"data": [...]}
    if isinstance(raw_data, dict) and "data" in raw_data:
        # print("dict")
        if isinstance(raw_data["data"], list):
            return raw_data["data"]

    # Case 3: Column-based JSON
    if isinstance(raw_data, dict):
        # print("dict 01")
        return convert_column_json_to_rows(raw_data)

    return []


def convert_column_json_to_rows(data: dict) -> list:

    # print("convert column json to rows")

    if not isinstance(data, dict):
        # print("is not dict")
        return []

    cleaned = {}

    for key, value in data.items():

        # unwrap [[...]]
        if isinstance(value, list) and len(value) == 1 and isinstance(value[0], list):
            cleaned[key] = value[0]
        else:
            cleaned[key] = value

    # Detect column-style lists
    list_lengths = [
        len(v) for v in cleaned.values()
        if isinstance(v, list)
    ]

    # print("list_lengths:", list_lengths)

    # 🔥 FIX: If no list found → treat as single row
    if not list_lengths:
        # print("No column lists found → treating as single row")
        return [cleaned]

    row_count = max(list_lengths)

    rows = []

    for i in range(row_count):
        row = {}

        for col, values in cleaned.items():

            if isinstance(values, list):
                row[col] = values[i] if i < len(values) else None
            else:
                row[col] = values

        rows.append(row)

    return rows


from datetime import datetime,timedelta


def build_history_prefix(date: datetime = None) -> str:
    if date is None:
        date = datetime.utcnow()

    return f"history/{date.year:04d}/{date.month:02d}/{date.day:02d}/"

def build_yesterday_history_prefix() -> str:
    """
    Generate R2 history prefix for yesterday.
    Format: history/YYYY/MM/DD/
    """
    yesterday = datetime.utcnow() - timedelta(days=1)

    return f"history/{yesterday.year:04d}/{yesterday.month:02d}/{yesterday.day:02d}/"

# def insert_transaction_between_range():
#
#     namespace = "POS_Transactions"
#     table_name = "Transaction_vars"
#     table_name_dump = "Transaction_vars"
#     bucket = "pos-transaction-imei"
#     # bucket = "pos-transaction"
#     # bucket = "pos-transaction"
#     # prefix = "dump/2026/01/29/"
#     prefix = build_yesterday_history_prefix()
#     batch_size = 200
#
#     start_time = time.time()
#     print_memory("start_time")
#     # ------------------------------------------------
#     # 1. Load Iceberg table
#     # ------------------------------------------------
#     catalog = get_catalog_client()
#     table = catalog.load_table(f"{namespace}.{table_name}")
#     arrow_schema = table.schema().as_arrow()
#
#     # ------------------------------------------------
#     # 2. Load existing pri_ids from Iceberg
#     # ------------------------------------------------
#     try:
#         existing_ids = set(
#             table.scan(selected_fields=["pri_id"])
#                  .to_arrow()["pri_id"]
#                  .to_pylist()
#         )
#
#     except Exception:
#         existing_ids = set()
#
#     print(f"Loaded {len(existing_ids)} existing IDs")
#
#     # ------------------------------------------------
#     # 3. List R2 files
#     # ------------------------------------------------
#     print_memory("before bucket")
#     keys = list_json_files(bucket=bucket, prefix=prefix)
#     print("data:",len(keys))
#     if not keys:
#         return {"success": True, "message": "No files found"}
#
#     total_inserted = 0
#     total_files = len(keys)
#
#     # ------------------------------------------------
#     # 4. Process in batches
#     # ------------------------------------------------
#     print("counting...")
#     current_pri_id = get_last_pri_id(namespace, table_name_dump)
#     # print("current_pri_id",current_pri_id)
#     time.sleep(10)
#     for i in range(0, total_files, batch_size):
#
#         batch_keys = keys[i:i + batch_size]
#         batch_rows = []
#         seen_batch_ids = set()
#         # print(keys)
#         # Download parallel
#         with ThreadPoolExecutor(max_workers=10) as executor:
#             futures = [
#                 executor.submit(get_r2_client().get_object, Bucket=bucket, Key=key)
#                 for key in batch_keys
#
#             ]
#
#             for future in as_completed(futures):
#                 try:
#                     obj = future.result()
#                     raw_data = json.loads(obj["Body"].read())
#                     # print("raw-data",raw_data)
#                     rows_from_file = extract_rows_from_json(raw_data)
#                     # print("return-data:",rows_from_file)
#                     for row in rows_from_file:
#
#                         uuid_val = row.get("pri_id")
#                         if not uuid_val:
#                             continue
#
#                         # Skip if duplicate inside batch
#                         if uuid_val in seen_batch_ids:
#                             continue
#
#                         # Skip if already in Iceberg
#                         if uuid_val in existing_ids:
#                             continue
#                         current_pri_id += 1
#                         row["pri_id_backup"] = current_pri_id
#                         row["pri_id"] = uuid_val
#                         # print(row["pri_id_backup"])
#                         batch_rows.append(row)
#                         seen_batch_ids.add(uuid_val)
#
#                 except Exception as e:
#                     print(f"File processing error: {e}")
#         # print(batch_rows)
#         print_memory(f"inserted {len(batch_rows)} rows")
#         if not batch_rows:
#             continue
#
#         # ------------------------------------------------
#         # 5. Clean rows
#         # ------------------------------------------------
#         cleaned = clean_rows(
#             rows=batch_rows,
#             boolean_fields=BOOLEAN_FIELDS,
#             timestamps_fields=TIMESTAMP_FIELDS,
#             field_overrides=FIELD_OVERRIDES
#         )
#
#         if not cleaned:
#             continue
#
#         # ------------------------------------------------
#         # 6. Convert to Arrow
#         # ------------------------------------------------
#         # print("cleaned")
#         # print(cleaned)
#
#         arrow_table, errors = process_chunk(cleaned, arrow_schema)
#
#         # print("Before append rows:", arrow_table.num_rows)
#         # print(
#         #     arrow_table.column("pri_id").to_pylist()
#         # )
#         print_memory("before append")
#         if arrow_table and arrow_table.num_rows > 0:
#             # print("Before append rows:", arrow_table.num_rows)
#             table.append(arrow_table)
#             total_inserted += arrow_table.num_rows
#
#             # Update dedupe cache
#             existing_ids.update(
#                 arrow_table.column("pri_id").to_pylist()
#             )
#
#             # print(f"Inserted {arrow_table.num_rows} rows")
#     end_time = time.time() - start_time
#     total_time = end_time - start_time
#     print(f"Inserted {total_inserted} rows in {end_time} seconds")
#     return {
#         "success": True,
#         "files_processed": total_files,
#         "rows_inserted": total_inserted,
#         "start_time":start_time,
#         "end_time":end_time,
#         "total_time_sec": round(total_time, 2)
#     }


# def insert_transaction_between_range():
#
#     namespace = "POS_Transactions"
#     table_name = "Transaction_vars"
#     bucket = "pos-transaction-imei"
#     prefix = build_yesterday_history_prefix()
#     batch_size = 200
#
#     start_time = time.perf_counter()
#     print_memory("start")
#
#     catalog = get_catalog_client()
#     table = catalog.load_table(f"{namespace}.{table_name}")
#     arrow_schema = table.schema().as_arrow()
#
#     print_memory("before bucket")
#     keys = list_json_files(bucket=bucket, prefix=prefix)
#
#     if not keys:
#         return {"success": True, "message": "No files found"}
#
#     total_inserted = 0
#     total_files = len(keys)
#
#     current_pri_id = get_last_pri_id(namespace, table_name)
#
#     for i in range(0, total_files, batch_size):
#
#         batch_keys = keys[i:i + batch_size]
#         batch_rows = []
#         seen_batch_ids = set()
#
#         with ThreadPoolExecutor(max_workers=10) as executor:
#             futures = [
#                 executor.submit(get_r2_client().get_object, Bucket=bucket, Key=key)
#                 for key in batch_keys
#             ]
#
#             for future in as_completed(futures):
#                 try:
#                     obj = future.result()
#                     raw_data = json.loads(obj["Body"].read())
#                     rows_from_file = extract_rows_from_json(raw_data)
#
#                     for row in rows_from_file:
#
#                         uuid_val = row.get("pri_id")
#                         if not uuid_val:
#                             continue
#
#                         if uuid_val in seen_batch_ids:
#                             continue
#
#                         current_pri_id += 1
#                         row["pri_id_backup"] = current_pri_id
#                         row["pri_id"] = uuid_val
#
#                         batch_rows.append(row)
#                         seen_batch_ids.add(uuid_val)
#
#                 except Exception as e:
#                     print(f"File processing error: {e}")
#
#         print_memory(f"batch rows: {len(batch_rows)}")
#
#         if not batch_rows:
#             continue
#
#         cleaned = clean_rows(
#             rows=batch_rows,
#             boolean_fields=BOOLEAN_FIELDS,
#             timestamps_fields=TIMESTAMP_FIELDS,
#             field_overrides=FIELD_OVERRIDES
#         )
#
#         arrow_table, errors = process_chunk(cleaned, arrow_schema)
#
#         print_memory("before append")
#
#         if arrow_table and arrow_table.num_rows > 0:
#             table.append(arrow_table)
#             total_inserted += arrow_table.num_rows
#
#     # Update high watermark
#     update_last_pri_id(namespace, table_name, current_pri_id)
#
#     end_time = time.perf_counter()
#     total_time = end_time - start_time
#
#     print(f"Inserted {total_inserted} rows in {total_time:.2f} seconds")
#
#     return {
#         "success": True,
#         "files_processed": total_files,
#         "rows_inserted": total_inserted,
#         "total_time_sec": round(total_time, 2)
#     }
#
# if __name__ == "__main__":
#     result = insert_transaction_between_range()
#     print(result)
    # result = build_yesterday_history_prefix()
    # print(result)

# def insert_transaction_between_range():
#
#     namespace = "POS_Transactions"
#     table_name = "Transaction_vars"
#     bucket = "pos-transaction-imei"
#     prefix = build_yesterday_history_prefix()
#     batch_size = 200
#
#     # -----------------------------
#     # Start timer
#     # -----------------------------
#     start_time = time.perf_counter()
#     print_memory("start")
#
#     # -----------------------------
#     # Load Iceberg table
#     # -----------------------------
#     catalog = get_catalog_client()
#     table = catalog.load_table(f"{namespace}.{table_name}")
#     arrow_schema = table.schema().as_arrow()
#
#     # -----------------------------
#     # List R2 files
#     # -----------------------------
#     print_memory("before bucket")
#     keys = list_json_files(bucket=bucket, prefix=prefix)
#
#     if not keys:
#         return {"success": True, "message": "No files found"}
#
#     total_inserted = 0
#     total_files = len(keys)
#
#     # Get last high watermark (ID)
#     current_pri_id = get_last_pri_id(namespace, table_name)
#
#     # -----------------------------
#     # Use one executor (IMPORTANT)
#     # -----------------------------
#     with ThreadPoolExecutor(max_workers=5) as executor:
#
#         for i in range(0, total_files, batch_size):
#
#             batch_keys = keys[i:i + batch_size]
#             batch_rows = []
#             seen_batch_ids = set()
#
#             # -----------------------------
#             # Download files in parallel
#             # -----------------------------
#             futures = [
#                 executor.submit(
#                     get_r2_client().get_object,
#                     Bucket=bucket,
#                     Key=key
#                 )
#                 for key in batch_keys
#             ]
#
#             for future in as_completed(futures):
#                 try:
#                     obj = future.result()
#
#                     # Read JSON safely
#                     raw_data = json.loads(obj["Body"].read())
#                     rows = extract_rows_from_json(raw_data)
#
#                     for row in rows:
#                         uuid_val = row.get("pri_id")
#                         if not uuid_val:
#                             continue
#
#                         # Batch-level dedupe only
#                         if uuid_val in seen_batch_ids:
#                             continue
#
#                         current_pri_id += 1
#                         row["pri_id_backup"] = current_pri_id
#                         row["pri_id"] = uuid_val
#
#                         batch_rows.append(row)
#                         seen_batch_ids.add(uuid_val)
#
#                 except Exception as e:
#                     print(f"File processing error: {e}")
#
#             print_memory(f"batch rows: {len(batch_rows)}")
#
#             if not batch_rows:
#                 continue
#
#             # -----------------------------
#             # Clean rows
#             # -----------------------------
#             cleaned = clean_rows(
#                 rows=batch_rows,
#                 boolean_fields=BOOLEAN_FIELDS,
#                 timestamps_fields=TIMESTAMP_FIELDS,
#                 field_overrides=FIELD_OVERRIDES
#             )
#
#             if not cleaned:
#                 continue
#
#             # -----------------------------
#             # Convert to Arrow
#             # -----------------------------
#             arrow_table, errors = process_chunk(cleaned, arrow_schema)
#
#             print_memory("before append")
#
#             if arrow_table and arrow_table.num_rows > 0:
#                 table.append(arrow_table)
#                 total_inserted += arrow_table.num_rows
#
#             # -----------------------------
#             # VERY IMPORTANT: Free Memory
#             # -----------------------------
#             del batch_rows
#             del cleaned
#             del arrow_table
#             gc.collect()
#
#             print_memory("after gc")
#
#     # -----------------------------
#     # Update high watermark
#     # -----------------------------
#     update_last_pri_id(namespace, table_name, current_pri_id)
#
#     end_time = time.perf_counter()
#     total_time = end_time - start_time
#
#     print(f"Inserted {total_inserted} rows in {total_time:.2f} seconds")
#
#     return {
#         "success": True,
#         "files_processed": total_files,
#         "rows_inserted": total_inserted,
#         "total_time_sec": round(total_time, 2)
#     }
#
#
# if __name__ == "__main__":
#     result = insert_transaction_between_range()
#     print(result)

def insert_transaction_between_range():

    namespace = "POS_Transactions"
    table_name = "Transaction_vars"
    bucket = "pos-transaction-imei"
    prefix = build_yesterday_history_prefix()
    batch_size = 200
    last_value = ""
    start_time = time.perf_counter()
    print_memory("start")

    catalog = get_catalog_client()
    print_memory("connect client")
    table = catalog.load_table(f"{namespace}.{table_name}")
    print_memory("before arrow")
    arrow_schema = table.schema().as_arrow()
    print_memory("after arrow")
    print_memory("before current_pri_id")
    # current_pri_id = 42826181
    current_pri_id = get_last_value("pos_transactions", "ingestion_tracking")
    print(f"current_pri_id: {current_pri_id}")
    # current_pri_id = get_last_column_name("pos_transactions", "ingestion_tracking")
    print(f"current_pri_id: {current_pri_id}")
    print(type(current_pri_id))
    print_memory("after current_pri_id")
    total_inserted = 0
    total_files = 0

    batch_keys = []
    print_memory("before Threadpool")
    with ThreadPoolExecutor(max_workers=2) as executor:
        print_memory("before bucket")
        # STREAM KEYS (no full list in memory)

        for key in iter_json_files(bucket, prefix):

            batch_keys.append(key)
            total_files += 1

            if len(batch_keys) < batch_size:
                continue

            # -----------------------------
            # Process Batch
            # -----------------------------
            print_memory("before process")
            total_inserted += process_batch(
                executor,
                batch_keys,
                bucket,
                table,
                arrow_schema,
                current_pri_id
            )

            batch_keys = []
            gc.collect()
            print_memory("after gc")
            print("-" * 100)
            # update_last_pri_id("POS_Transactions", "ingestion_tracking", current_pri_id)
        # Process remaining keys
        print_memory("before batch")
        if batch_keys:
            total_inserted += process_batch(
                executor,
                batch_keys,
                bucket,
                table,
                arrow_schema,
                current_pri_id
            )

    update_last_pri_id("POS_Transactions", "ingestion_tracking", current_pri_id)

    total_time = time.perf_counter() - start_time

    print(f"Inserted {total_inserted} rows in {total_time:.2f} seconds")

    return {
        "success": True,
        "files_processed": total_files,
        "rows_inserted": total_inserted,
        "total_time_sec": round(total_time, 2)
    }

if __name__ == "__main__":
    result = insert_transaction_between_range()
    print(result)