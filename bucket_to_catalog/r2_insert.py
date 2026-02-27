import time
from bucket_to_catalog.table_utility import clean_rows, process_chunk
from bucket_to_catalog.transaction_utility import *
from core.catalog_client import get_catalog_client
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyiceberg.catalog import NoSuchTableError
from core.logger import get_logger
from bucket_to_catalog.error_handler import handle_ingestion_error
from bucket_to_catalog.read_bucket import list_json_files
from core.r2_client import get_r2_client
from routers.bucket_to_r2Catalog import get_last_pri_id
import json
import gc
import psutil
import os

logger = get_logger(__name__)

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

def insert_transaction_between_range():
    namespace = "POS_Transactions"
    table_name = "Transaction_vars"
    bucket = "pos-transaction-imei"
    prefix = build_yesterday_history_prefix()  # yesterday or custom
    batch_size = 20  # reduced for memory safety
    print("Start...")
    start_time = time.time()

    # ------------------------------------------------
    # 1. Load Iceberg table
    # ------------------------------------------------
    catalog = get_catalog_client()
    table = catalog.load_table(f"{namespace}.{table_name}")
    arrow_schema = table.schema().as_arrow()

    # ------------------------------------------------
    # 2. Get starting backup ID
    # ------------------------------------------------
    current_pri_id = get_last_pri_id(namespace, table_name)

    # ------------------------------------------------
    # 3. List R2 files
    # ------------------------------------------------
    keys = list_json_files(bucket=bucket, prefix=prefix)

    if not keys:
        return {"success": True, "message": "No files found"}

    total_inserted = 0
    total_files = len(keys)

    # ------------------------------------------------
    # 4. Process in small batches
    # ------------------------------------------------
    for i in range(0, total_files, batch_size):

        batch_keys = keys[i:i + batch_size]
        print(i)
        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = [
                executor.submit(get_r2_client().get_object, Bucket=bucket, Key=key)
                for key in batch_keys
            ]

            for future in as_completed(futures):

                try:
                    obj = future.result()
                    raw_data = json.loads(obj["Body"].read())
                    rows = extract_rows_from_json(raw_data)

                    if not rows:
                        continue

                    batch_rows = []

                    for row in rows:
                        uuid_val = row.get("pri_id")
                        if not uuid_val:
                            continue

                        current_pri_id += 1
                        row["pri_id_backup"] = current_pri_id
                        print(current_pri_id)
                        row["pri_id"] = uuid_val
                        batch_rows.append(row)

                    if not batch_rows:
                        continue

                    # ----------------------------
                    # Clean rows
                    # ----------------------------
                    cleaned = clean_rows(
                        rows=batch_rows,
                        boolean_fields=BOOLEAN_FIELDS,
                        timestamps_fields=TIMESTAMP_FIELDS,
                        field_overrides=FIELD_OVERRIDES
                    )

                    if not cleaned:
                        continue

                    # ----------------------------
                    # Convert to Arrow
                    # ----------------------------
                    arrow_table, errors = process_chunk(cleaned, arrow_schema)

                    if arrow_table and arrow_table.num_rows > 0:
                        table.append(arrow_table)
                        total_inserted += arrow_table.num_rows

                    # ----------------------------
                    # Memory Cleanup
                    # ----------------------------
                    del batch_rows
                    del cleaned
                    del arrow_table
                    gc.collect()

                except Exception as e:
                    print(f"File processing error: {e}")

    return {
        "success": True,
        "files_processed": total_files,
        "rows_inserted": total_inserted,
        "time_taken_sec": round(time.time() - start_time, 2)
    }


if __name__ == "__main__":
    result = insert_transaction_between_range()
    print(result)
    # result = build_yesterday_history_prefix()
    # print(result)

# def insert_transaction_between_range():
#
#     namespace = "POS_Transactions"
#     table_name = "Transaction_vars"
#     bucket = "pos-transaction-imei"
#     prefix = build_yesterday_history_prefix()
#     batch_size = 20
#
#     start_time = time.time()
#
#     print("=" * 60)
#     print("🚀 Starting Transaction Ingestion")
#     print("Namespace :", namespace)
#     print("Table     :", table_name)
#     print("Bucket    :", bucket)
#     print("Prefix    :", prefix)
#     print("=" * 60)
#
#     # ------------------------------------------------
#     # 1. Load Iceberg table
#     # ------------------------------------------------
#     print("🔹 Loading Iceberg table...")
#     catalog = get_catalog_client()
#     table = catalog.load_table(f"{namespace}.{table_name}")
#     arrow_schema = table.schema().as_arrow()
#     print("✅ Table loaded successfully")
#
#     # ------------------------------------------------
#     # 2. Get starting backup ID
#     # ------------------------------------------------
#     current_pri_id = get_last_pri_id(namespace, table_name)
#     print(f"🔹 Starting pri_id_backup from: {current_pri_id}")
#
#     # ------------------------------------------------
#     # 3. List R2 files
#     # ------------------------------------------------
#     print("🔹 Listing files from R2...")
#     keys = list_json_files(bucket=bucket, prefix=prefix)
#
#     if not keys:
#         print("⚠ No files found for prefix:", prefix)
#         return {"success": True, "message": "No files found"}
#
#     total_inserted = 0
#     total_files = len(keys)
#
#     print(f"✅ Total files found: {total_files}")
#     print("=" * 60)
#
#     # ------------------------------------------------
#     # 4. Process in batches
#     # ------------------------------------------------
#     for i in range(0, total_files, batch_size):
#
#         batch_keys = keys[i:i + batch_size]
#         print(f"\n📦 Processing Batch {i // batch_size + 1}")
#         print(f"Files in this batch: {len(batch_keys)}")
#
#         with ThreadPoolExecutor(max_workers=4) as executor:
#             futures = [
#                 executor.submit(get_r2_client().get_object, Bucket=bucket, Key=key)
#                 for key in batch_keys
#             ]
#
#             for future in as_completed(futures):
#
#                 try:
#                     obj = future.result()
#                     raw_data = json.loads(obj["Body"].read())
#                     rows = extract_rows_from_json(raw_data)
#
#                     print(f"   📄 Extracted rows: {len(rows)}")
#
#                     if not rows:
#                         continue
#
#                     batch_rows = []
#
#                     for row in rows:
#                         uuid_val = row.get("pri_id")
#                         if not uuid_val:
#                             continue
#
#                         current_pri_id += 1
#                         row["pri_id_backup"] = current_pri_id
#                         row["pri_id"] = uuid_val
#                         batch_rows.append(row)
#
#                     print(f"   🧹 Valid rows after filter: {len(batch_rows)}")
#
#                     if not batch_rows:
#                         continue
#
#                     # ----------------------------
#                     # Clean rows
#                     # ----------------------------
#                     cleaned = clean_rows(
#                         rows=batch_rows,
#                         boolean_fields=BOOLEAN_FIELDS,
#                         timestamps_fields=TIMESTAMP_FIELDS,
#                         field_overrides=FIELD_OVERRIDES
#                     )
#
#                     print(f"   🧽 Rows after cleaning: {len(cleaned)}")
#
#                     if not cleaned:
#                         continue
#
#                     # ----------------------------
#                     # Convert to Arrow
#                     # ----------------------------
#                     arrow_table, errors = process_chunk(cleaned, arrow_schema)
#
#                     if arrow_table and arrow_table.num_rows > 0:
#                         table.append(arrow_table)
#                         total_inserted += arrow_table.num_rows
#
#                         print(f"   ✅ Inserted rows: {arrow_table.num_rows}")
#                         print(f"   📊 Total inserted so far: {total_inserted}")
#
#                     # ----------------------------
#                     # Memory Cleanup
#                     # ----------------------------
#                     del batch_rows
#                     del cleaned
#                     del arrow_table
#                     gc.collect()
#
#                 except Exception as e:
#                     print(f"❌ File processing error: {e}")
#
#     print("\n" + "=" * 60)
#     print("🎉 Ingestion Completed")
#     print("Total Files Processed :", total_files)
#     print("Total Rows Inserted   :", total_inserted)
#     print("Time Taken (seconds)  :", round(time.time() - start_time, 2))
#     print("=" * 60)
#
#     return {
#         "success": True,
#         "files_processed": total_files,
#         "rows_inserted": total_inserted,
#         "time_taken_sec": round(time.time() - start_time, 2)
#     }


# if __name__ == "__main__":
#     result = insert_transaction_between_range()
#     print("\nFinal Result:", result)

# cloud job test
# def insert_transaction_between_range():
#
#     namespace = "POS_Transactions"
#     table_name = "Transaction_vars"
#     bucket = "pos-transaction-imei"
#     prefix = build_history_prefix()
#     batch_size = 200
#
#     start_time = time.time()
#
#     print("=" * 60)
#     print("🚀 Starting Transaction Ingestion")
#     print("=" * 60)
#
#     print_memory("Start")
#
#     catalog = get_catalog_client()
#     table = catalog.load_table(f"{namespace}.{table_name}")
#     arrow_schema = table.schema().as_arrow()
#
#     print_memory("After Table Load")
#
#     current_pri_id = 42821386
#
#     keys = list_json_files(bucket=bucket, prefix=prefix)
#     print("=" * 60)
#     print("total-count",len(keys))
#     print("=" * 60)
#     print(keys)
#     if not keys:
#         print("⚠ No files found")
#         return {"success": True}
#
#     total_inserted = 0
#     total_files = len(keys)
#
#     for i in range(0, total_files, batch_size):
#
#         print("\n📦 New Batch Started")
#         print_memory("Before Batch")
#
#         batch_keys = keys[i:i + batch_size]
#         print(i)
#         with ThreadPoolExecutor(max_workers=10) as executor:
#             futures = [
#                 executor.submit(get_r2_client().get_object, Bucket=bucket, Key=key)
#                 for key in batch_keys
#             ]
#
#             for future in as_completed(futures):
#
#                 try:
#                     obj = future.result()
#                     raw_data = json.loads(obj["Body"].read())
#                     rows = extract_rows_from_json(raw_data)
#
#                     if not rows:
#                         continue
#
#                     batch_rows = []
#
#                     for row in rows:
#                         uuid_val = row.get("pri_id")
#                         if not uuid_val:
#                             continue
#
#                         current_pri_id += 1
#                         row["pri_id_backup"] = current_pri_id
#                         print(row["pri_id_backup"])
#                         batch_rows.append(row)
#
#                     cleaned = clean_rows(
#                         rows=batch_rows,
#                         boolean_fields=BOOLEAN_FIELDS,
#                         timestamps_fields=TIMESTAMP_FIELDS,
#                         field_overrides=FIELD_OVERRIDES
#                     )
#
#                     arrow_table, errors = process_chunk(cleaned, arrow_schema)
#
#                     if arrow_table and arrow_table.num_rows > 0:
#                         table.append(arrow_table)
#                         total_inserted += arrow_table.num_rows
#
#                     # Memory cleanup
#                     del batch_rows
#                     del cleaned
#                     del arrow_table
#                     gc.collect()
#
#                 except Exception as e:
#                     print(f"❌ Error: {e}")
#
#         print_memory("After Batch")
#
#     print("\n🎉 Ingestion Completed")
#     print_memory("End")
#
#     return {
#         "success": True,
#         "files_processed": total_files,
#         "rows_inserted": total_inserted,
#         "time_taken_sec": round(time.time() - start_time, 2)
#     }

if __name__ == "__main__":
    result = insert_transaction_between_range()
    print("\nFinal Result:", result)
