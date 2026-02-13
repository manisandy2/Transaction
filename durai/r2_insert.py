from fastapi import APIRouter, Query, Body, HTTPException
import time
from typing import Optional, List, Dict, Any
# Fixed imports
from utility import schema
from table_utility import clean_rows, process_chunk
from transaction_utility import (REQUIRED_FIELDS, FIELD_OVERRIDES,
                                  VARCHAR_FIELDS, TIMESTAMP_FIELDS, BOOLEAN_FIELDS)
from core.catalog_client import get_catalog_client
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyiceberg.catalog import NoSuchTableError
from core.logger import get_logger
from error_handler import handle_ingestion_error
from read_bucket import list_json_files

logger = get_logger(__name__)
#
# def insert_transaction_between_range():
#     # Configuration
#     namespace, table_name = "POS_Transactions", "Transaction_vars_2026-02-13"
#     bucket = "pos-transaction-imei"
#     prefix = "history/2026/01/29/"
#
#     # Initialize start time
#     total_start = time.time()
#
#     logger.info(f"Listing objects in {bucket}/{prefix}...")
#     try:
#         keys = list(list_objects_from_r2(bucket, prefix))
#     except Exception as e:
#         logger.error(f"Failed to list objects: {e}")
#         return {"success": False, "error": str(e)}
#
#     logger.info(f"Found {len(keys)} objects. Starting download...")
#
#     # 1. Load Iceberg Table
#     try:
#         catalog = get_catalog_client()
#         table_identifier = f"{namespace}.{table_name}"
#         tbl = catalog.load_table(table_identifier)
#         logger.info(f"Iceberg table loaded successfully")
#     except NoSuchTableError:
#         logger.error("Iceberg table not found")
#         raise HTTPException(status_code=404, detail=f"Table not found")
#     except Exception as e:
#         logger.exception("Iceberg table load failed")
#         raise HTTPException(status_code=500, detail=str(e))
#
#     # 2. Iterate in Batches
#     batch_index = 0
#     total_rows_fetched = 0
#     total_rows_successful = 0
#     failed_batches = []
#
#     execution_stats = {
#         "fetch_time": 0.0,
#         "clean_time": 0.0,
#         "arrow_time": 0.0,
#         "append_time": 0.0
#     }
#
#     # Process keys in batches (e.g., 20 files at a time)
#     batch_size = 1000
#
#     for i in range(0, len(keys), batch_size):
#         batch_keys = keys[i : i + batch_size]
#         batch_index += 1
#         logger.info(f"Processing batch {batch_index}: {len(batch_keys)} files")
#
#         batch_records = []
#
#         # 2a. Fetch Objects (Parallel)
#         t_start_fetch = time.time()
#         try:
#             with ThreadPoolExecutor(max_workers=10) as executor:
#                 futures = [executor.submit(process_object, bucket, key) for key in batch_keys]
#
#                 for future in as_completed(futures):
#                     result = future.result()
#                     if result and result.get("data"):
#                         # Assuming data is a list of records
#                         data = result["data"]
#                         if isinstance(data, list):
#                             batch_records.extend(data)
#                         elif isinstance(data, dict):
#                             batch_records.append(data)
#         except Exception as e:
#             logger.error(f"Error fetching batch {batch_index}: {e}")
#             failed_batches.append({"batch_index": batch_index, "stage": "fetch", "error": str(e)})
#             continue
#
#         execution_stats["fetch_time"] += (time.time() - t_start_fetch)
#
#         if not batch_records:
#             continue
#
#         total_rows_fetched += len(batch_records)
#
#         # 2b. Clean Rows
#         t_start_clean = time.time()
#         try:
#             cleaned_data = clean_rows(
#                 batch_records,
#                 boolean_fields=BOOLEAN_FIELDS,
#                 timestamps_fields=TIMESTAMP_FIELDS,
#                 field_overrides=FIELD_OVERRIDES
#             )
#         except Exception as e:
#             logger.error(f"Error cleaning batch {batch_index}: {e}")
#             failed_batches.append({"batch_index": batch_index, "stage": "clean", "error": str(e)})
#             continue
#         execution_stats["clean_time"] += (time.time() - t_start_clean)
#
#         # 2c. Create Arrow Table
#         t_start_arrow = time.time()
#         try:
#             # Generate schema from first record if needed, but we should probably use table schema
#             # Using schema() from utility to generate arrow schema based on a sample record
#             if not cleaned_data:
#                 continue
#
#             _, arrow_schema = schema(cleaned_data[0], REQUIRED_FIELDS, FIELD_OVERRIDES)
#
#             # --- DEBUG: Inspect pri_id schema ---
#             try:
#                 pri_field = arrow_schema.field("pri_id")
#                 logger.info(f"Schema check: field='pri_id', nullable={pri_field.nullable}, type={pri_field.type}")
#             except Exception as e:
#                 logger.warning(f"Schema check failed for pri_id: {e}")
#             # ------------------------------------
#
#             pa_table, row_errors = process_chunk(cleaned_data, arrow_schema)
#
#             if row_errors:
#                 logger.warning(f"Batch {batch_index} had {len(row_errors)} row errors")
#                 # Handle row errors if needed (already logged by process_chunk)
#
#         except Exception as e:
#             logger.error(f"Error creating Arrow table for batch {batch_index}: {e}")
#             failed_batches.append({"batch_index": batch_index, "stage": "arrow_conversion", "error": str(e)})
#             continue
#         execution_stats["arrow_time"] += (time.time() - t_start_arrow)
#
#         # 2d. Append to Iceberg
#         t_start_append = time.time()
#         try:
#             if pa_table.num_rows > 0:
#                 print(f"Adding batch {batch_index} with {pa_table.num_rows} rows to Iceberg...")
#                 tbl.append(pa_table)
#                 total_rows_successful += pa_table.num_rows
#                 logger.info(f"Batch {batch_index} appended {pa_table.num_rows} rows")
#             else:
#                 logger.warning(f"Batch {batch_index} resulted in 0 rows (all skipped due to errors)")
#         except Exception as e:
#             logger.error(f"Error appending batch {batch_index} to Iceberg: {e}")
#             failed_batches.append({"batch_index": batch_index, "stage": "append", "error": str(e)})
#         execution_stats["append_time"] += (time.time() - t_start_append)
#
#
#     total_end = time.time()
#
#     # Handle failed batches summary logging
#     if failed_batches:
#         logger.warning(f"{len(failed_batches)} batches failed during ingestion")
#         failed_records_summary = [
#             {"batch": b["batch_index"], "stage": b["stage"], "error": b["error"]}
#             for b in failed_batches
#         ]
#         logger.info(f"Failed batches summary: {failed_records_summary}")
#
#     logger.info(
#         f"END ingestion | fetched={total_rows_fetched} successful={total_rows_successful} "
#         f"failed_batches={len(failed_batches)} total_time={total_end - total_start:.2f}s"
#     )
#
#     response = {
#         "success": True,
#         "message": "Data ingestion completed (batched)",
#         "rows_fetched": total_rows_fetched,
#         "rows_successful": total_rows_successful,
#         "total_batches_processed": batch_index,
#         "batches_failed": len(failed_batches),
#         "execution_stats": {k: round(v, 2) for k, v in execution_stats.items()},
#         "total_time": round(total_end - total_start, 2),
#     }
#
#     if failed_batches:
#         response["failed_batches_summary"] = [
#             {"batch": b["batch_index"], "stage": b["stage"], "error": b["error"]}
#             for b in failed_batches
#         ]
#
#     return response
#
# print(insert_transaction_between_range())

# def insert_transaction_between_range():
#
#     namespace = "POS_Transactions"
#     table_name = "Transaction_vars_2026-02-13"
#     bucket = "pos-transaction-imei"
#     prefix = "history/2026/01/29/"
#     batch_size = 100
#
#     total_start = time.time()
#
#     # -----------------------------------------
#     # 1. Load Iceberg Table
#     # -----------------------------------------
#     try:
#         catalog = get_catalog_client()
#         tbl = catalog.load_table(f"{namespace}.{table_name}")
#         logger.info("Iceberg table loaded successfully")
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))
#
#     # Always use Iceberg schema
#     arrow_schema = tbl.schema().as_arrow()
#
#     # -----------------------------------------
#     # 2. Load existing pri_ids (for dedupe)
#     # -----------------------------------------
#     logger.info("Loading existing pri_id values for deduplication...")
#     try:
#         existing_ids = set(
#             tbl.scan(selected_fields=["pri_id"])
#                .to_arrow()["pri_id"]
#                .to_pylist()
#         )
#     except Exception:
#         existing_ids = set()
#
#     # -----------------------------------------
#     # 3. List R2 files
#     # -----------------------------------------
#     try:
#         keys = list(list_objects_from_r2(bucket, prefix))
#     except Exception as e:
#         return {"success": False, "error": str(e)}
#
#     logger.info(f"Found {len(keys)} files")
#
#     total_rows_fetched = 0
#     total_rows_inserted = 0
#
#     # -----------------------------------------
#     # 4. Process in batches
#     # -----------------------------------------
#     for i in range(0, len(keys), batch_size):
#
#         batch_keys = keys[i:i + batch_size]
#         batch_records = []
#
#         # ---- Download files (parallel) ----
#         with ThreadPoolExecutor(max_workers=10) as executor:
#             futures = [executor.submit(process_object, bucket, key) for key in batch_keys]
#
#             for future in as_completed(futures):
#                 result = future.result()
#                 if result and result.get("data"):
#                     data = result["data"]
#                     if isinstance(data, list):
#                         batch_records.extend(data)
#                     else:
#                         batch_records.append(data)
#
#         if not batch_records:
#             continue
#
#         total_rows_fetched += len(batch_records)
#
#         # -----------------------------------------
#         # 5. Clean data
#         # -----------------------------------------
#         cleaned = clean_rows(
#             batch_records,
#             boolean_fields=BOOLEAN_FIELDS,
#             timestamps_fields=TIMESTAMP_FIELDS,
#             field_overrides=FIELD_OVERRIDES
#
#         )
#
#         # -----------------------------------------
#         # 6. Remove null pri_id
#         # -----------------------------------------
#         cleaned = [
#             row for row in cleaned
#             if row.get("pri_id")
#         ]
#
#         if not cleaned:
#             continue
#
#         # -----------------------------------------
#         # 7. Remove duplicates
#         # -----------------------------------------
#         new_rows = [
#             row for row in cleaned
#             if row["pri_id"] not in existing_ids
#         ]
#
#         if not new_rows:
#             logger.info("Batch skipped (all duplicates)")
#             continue
#
#         # -----------------------------------------
#         # 8. Convert to Arrow
#         # -----------------------------------------
#         pa_table, row_errors = process_chunk(new_rows, arrow_schema)
#
#         if pa_table.num_rows == 0:
#             continue
#
#         # -----------------------------------------
#         # 9. Append to Iceberg
#         # -----------------------------------------
#         tbl.append(pa_table)
#         total_rows_inserted += pa_table.num_rows
#
#         # Update dedupe cache
#         existing_ids.update(
#             pa_table.column("pri_id").to_pylist()
#         )
#
#         logger.info(f"Inserted {pa_table.num_rows} rows")
#
#     # -----------------------------------------
#     # Final Response
#     # -----------------------------------------
#     return {
#         "success": True,
#         "rows_fetched": total_rows_fetched,
#         "rows_inserted": total_rows_inserted,
#         "time_taken_sec": round(time.time() - total_start, 2)
#     }

# def get_total_files(bucket: str, prefix: str = ""):
#     try:
#         keys = list(list_objects_from_r2(bucket, prefix))
#         total_files = len(keys)
#
#         return {
#             "bucket": bucket,
#             "prefix": prefix,
#             "total_files": total_files
#         }
#
#     except Exception as e:
#         return {"error": str(e)}
#
# result = get_total_files("pos-transaction-imei", "history/2026/01/29/")
# print(result)

# result = insert_transaction_between_range()
# print(result)

# def insert_transaction_between_range():
#
#     namespace = "POS_Transactions"
#     table_name = "Transaction_vars_2026-02-13"
#     bucket = "pos-transaction-imei"
#     prefix = "history/2026/01/29/"
#
#     # Batch size for DOWNLOAD ONLY
#     download_batch_size = 100
#
#     start_time = time.time()
#
#     # ------------------------------
#     # 1. Load Iceberg Table
#     # ------------------------------
#     logger.info(f"Loading table {namespace}.{table_name}...")
#     try:
#         catalog = get_catalog_client()
#         table = catalog.load_table(f"{namespace}.{table_name}")
#         arrow_schema = table.schema().as_arrow()
#     except Exception as e:
#         logger.error(f"Failed to load table: {e}")
#         raise HTTPException(status_code=500, detail=str(e))
#
#     # ------------------------------
#     # 2. Load existing pri_ids
#     # ------------------------------
#     logger.info("Loading existing pri_id values for deduplication...")
#     start_load_ids = time.time()
#     try:
#         existing_ids = set(
#             table.scan(selected_fields=["pri_id"])
#                  .to_arrow()["pri_id"]
#                  .to_pylist()
#         )
#         logger.info(f"Loaded {len(existing_ids)} existing IDs in {time.time() - start_load_ids:.2f}s")
#     except Exception as e:
#         logger.warning(f"Failed to load existing IDs (assuming empty table): {e}")
#         existing_ids = set()
#
#     # ------------------------------
#     # 3. List R2 files
#     # ------------------------------
#     logger.info(f"Listing objects in {bucket}/{prefix}...")
#     try:
#         keys = list(list_objects_from_r2(bucket, prefix))
#     except Exception as e:
#         logger.error(f"Failed to list objects: {e}")
#         return {"success": False, "error": str(e)}
#
#     total_files = len(keys)
#     logger.info(f"Found {total_files} files to process")
#
#     # ------------------------------
#     # 4. Fetch ALL Data
#     # ------------------------------
#     logger.info("Starting bulk download...")
#     all_records = []
#
#     for i in range(0, total_files, download_batch_size):
#         batch_keys = keys[i : i + download_batch_size]
#         logger.info(f"Downloading batch {i//download_batch_size + 1}/{(total_files//download_batch_size)+1} ({len(batch_keys)} files)...")
#
#         with ThreadPoolExecutor(max_workers=20) as executor:
#             futures = [executor.submit(process_object, bucket, key) for key in batch_keys]
#
#             for future in as_completed(futures):
#                 try:
#                     result = future.result()
#                     if result and result.get("data"):
#                         data = result["data"]
#                         if isinstance(data, list):
#                             all_records.extend(data)
#                         elif isinstance(data, dict):
#                             all_records.append(data)
#                 except Exception as e:
#                     logger.error(f"Error processing future: {e}")
#
#     total_fetched = len(all_records)
#     logger.info(f"Downloaded {total_fetched} records total.")
#
#     if total_fetched == 0:
#         return {"success": True, "message": "No records found in files."}
#
#     # ------------------------------
#     # 5. Clean Data (Usage of clean_rows)
#     # ------------------------------
#     logger.info("Cleaning records...")
#     t_clean_start = time.time()
#     try:
#         # clean_rows might be slow for huge lists, but requested "get all data"
#         cleaned_records = clean_rows(
#             all_records,
#             boolean_fields=BOOLEAN_FIELDS,
#             timestamps_fields=TIMESTAMP_FIELDS,
#             field_overrides=FIELD_OVERRIDES,
#         )
#     except Exception as e:
#         logger.error(f"Error cleaning records: {e}")
#         return {"success": False, "error": f"Cleaning failed: {str(e)}"}
#
#     logger.info(f"Cleaning completed in {time.time() - t_clean_start:.2f}s")
#
#     # ------------------------------
#     # 6. Global Deduplication
#     # ------------------------------
#     logger.info("Deduplicating...")
#     unique_map = {}
#
#     # Keep the LAST valid record for each pri_id (or first, depending on preference)
#     # Using dictionary to dedup by pri_id
#     for row in cleaned_records:
#         pid = row.get("pri_id")
#         if pid:
#             unique_map[pid] = row
#
#     # Filter against Iceberg
#     new_rows = []
#     for pid, row in unique_map.items():
#         if pid not in existing_ids:
#             new_rows.append(row)
#
#     logger.info(f"Records after dedup: {len(new_rows)} (Original: {total_fetched}, Unique: {len(unique_map)})")
#
#     if not new_rows:
#         return {"success": True, "message": "No new records to insert."}
#
#     # ------------------------------
#     # 7. Convert and Insert
#     # ------------------------------
#     logger.info("Converting to Arrow table...")
#     try:
#         pa_table, row_errors = process_chunk(new_rows, arrow_schema)
#
#         if row_errors:
#             logger.warning(f"Encountered {len(row_errors)} row errors during conversion")
#
#         if pa_table.num_rows > 0:
#             logger.info(f"Appending {pa_table.num_rows} rows to Iceberg...")
#             table.append(pa_table)
#             logger.info("Append successful.")
#         else:
#             logger.warning("No valid rows to append after conversion.")
#
#     except Exception as e:
#         logger.error(f"Insertion failed: {e}")
#         return {"success": False, "error": str(e)}
#
#     return {
#         "success": True,
#         "files_processed": total_files,
#         "records_fetched": total_fetched,
#         "records_unique": len(unique_map),
#         "records_inserted": pa_table.num_rows,
#         "time_taken_sec": round(time.time() - start_time, 2)
#     }
#
# result = insert_transaction_between_range()
# print(result)


# def normalize_row_types(rows, arrow_schema):
#     """
#     Convert JSON string values to proper types
#     based on Arrow schema before table creation.
#     """
#
#     schema_fields = {field.name: field.type for field in arrow_schema}
#
#     for row in rows:
#         for col, arrow_type in schema_fields.items():
#
#             value = row.get(col)
#
#             if value is None:
#                 continue
#
#             # ---- Integer / Long ----
#             if pa.types.is_integer(arrow_type):
#                 try:
#                     row[col] = int(value)
#                 except Exception:
#                     row[col] = None
#
#             # ---- Float / Double ----
#             elif pa.types.is_floating(arrow_type):
#                 try:
#                     row[col] = float(value)
#                 except Exception:
#                     row[col] = None
#
#             # ---- Boolean ----
#             elif pa.types.is_boolean(arrow_type):
#                 if str(value).lower() in ["1", "true"]:
#                     row[col] = True
#                 elif str(value).lower() in ["0", "false"]:
#                     row[col] = False
#                 else:
#                     row[col] = None
#
#             # ---- String ----
#             elif pa.types.is_string(arrow_type):
#                 row[col] = str(value)
#
#     return rows
#
# def convert_column_json_to_rows(data: dict) -> list:
#     """
#     Safely convert column-wise JSON to row-wise records.
#     Handles:
#     - double nested lists [[...]]
#     - single list [...]
#     - scalar values
#     """
#
#     cleaned = {}
#
#     # Step 1: Normalize values
#     for key, value in data.items():
#
#         # Case 1: [[...]] → unwrap outer list
#         if isinstance(value, list) and len(value) == 1 and isinstance(value[0], list):
#             cleaned[key] = value[0]
#
#         # Case 2: already list
#         elif isinstance(value, list):
#             cleaned[key] = value
#
#         # Case 3: scalar → repeat later
#         else:
#             cleaned[key] = value
#
#     # Step 2: Find max row count
#     list_lengths = [
#         len(v) for v in cleaned.values()
#         if isinstance(v, list)
#     ]
#
#     if not list_lengths:
#         return []
#
#     row_count = max(list_lengths)
#
#     # Step 3: Build rows safely
#     rows = []
#
#     for i in range(row_count):
#         row = {}
#
#         for col, values in cleaned.items():
#
#             if isinstance(values, list):
#                 # Safe index
#                 row[col] = values[i] if i < len(values) else None
#             else:
#                 # Scalar → same value for all rows
#                 row[col] = values
#
#         rows.append(row)
#
#     return rows
#
# from read_bucket import  list_json_files
# import json
# from core.r2_client import get_r2_client
# import pyarrow as pa
# import pandas as pd
#
# def insert_transaction_between_range():
#
#     namespace = "POS_Transactions"
#     table_name = "Transaction_vars_2026-02-13"
#     bucket = "pos-transaction-imei"
#     prefix = "history/2026/01/29/"
#     batch_size = 100
#
#     start_time = time.time()
#
#     # -----------------------------------------
#     # 1. Load Iceberg Table
#     # -----------------------------------------
#     logger.info(f"Loading table {namespace}.{table_name}...")
#
#     catalog = get_catalog_client()
#     table = catalog.load_table(f"{namespace}.{table_name}")
#     arrow_schema = table.schema().as_arrow()
#
#     # -----------------------------------------
#     # 2. Load existing pri_ids
#     # -----------------------------------------
#     logger.info("Loading existing pri_id values for deduplication...")
#
#     try:
#         existing_ids = set(
#             table.scan(selected_fields=["pri_id"])
#                  .to_arrow()["pri_id"]
#                  .to_pylist()
#         )
#     except Exception:
#         existing_ids = set()
#
#     logger.info(f"Loaded {len(existing_ids)} existing IDs")
#
#     # -----------------------------------------
#     # 3. List R2 files
#     # -----------------------------------------
#     keys = list_json_files(prefix="history/2026/01/29/",bucket="pos-transaction-imei")
#     print(keys)
#     total_files = len(keys)
#
#     logger.info(f"Found {total_files} files")
#     print(keys[0])
#
#     rows = []
#     appended_count = 0
#     for key in keys:
#
#         obj = get_r2_client().get_object(Bucket=bucket, Key=key)
#         raw_data = json.loads(obj["Body"].read())
#         rows_from_file = convert_column_json_to_rows(raw_data)
#         for row in rows_from_file:
#             pri_id = row.get("pri_id")
#
#             if not pri_id:
#                 continue
#
#             if pri_id in existing_ids:
#                 continue
#
#             existing_ids.add(pri_id)
#             rows.append(row)
#         appended_count += 1
#         print("count:",appended_count)
#         if appended_count == 100:
#             break
#         # Skip if duplicate
#         # pri_id = data.get("pri_id")
#         # if pri_id in existing_ids:
#         #     continue
#
#         # rows.append(data)
#         # existing_ids.add(pri_id)
#
#     print("RAW TYPE:", type(raw_data))
#     print("RAW KEYS:", raw_data.keys() if isinstance(raw_data, dict) else "NOT DICT")
#     print("count:",len(rows))
#     rows = clean_rows(
#         rows=rows,
#         boolean_fields=BOOLEAN_FIELDS,
#         timestamps_fields=TIMESTAMP_FIELDS,
#         field_overrides=FIELD_OVERRIDES
#     )
#
#     # iceberg_schema_obj, arrow_schema_obj = schema(
#     #     rows,
#     #     required_fields=REQUIRED_FIELDS,
#     #     field_overrides=FIELD_OVERRIDES
#     # )
#
#     # print("iceberg_schema_obj",iceberg_schema_obj)
#     # print("arrow_schema_obj",arrow_schema_obj)
#
#     arrow_table, conversion_errors = process_chunk(rows, arrow_schema)
#
#     print("arrow_table",arrow_table)
#     table.append(arrow_table)
#     # -----------------------------------------
#     # 4. Process in DOWNLOAD batches
#     # -----------------------------------------
#
#
#
#
#
#     #
#     #
#     #     # -----------------------------------------
#     #     # 5. Clean Batch
#     #     # -----------------------------------------
#     #     cleaned = clean_rows(
#     #         batch_records,
#     #         boolean_fields=BOOLEAN_FIELDS,
#     #         timestamps_fields=TIMESTAMP_FIELDS,
#     #         field_overrides=FIELD_OVERRIDES,
#     #     )
#     # #
#     #     # -----------------------------------------
#     #     # 6. Remove null pri_id
#     #     # -----------------------------------------
#     #     cleaned = [row for row in cleaned if row.get("pri_id")]
#     #
#     #     if not cleaned:
#     #         continue
#     # #
#     #     # -----------------------------------------
#     #     # 7. Deduplicate batch
#     #     # -----------------------------------------
#     #     new_rows = []
#     #     for row in cleaned:
#     #         pid = row["pri_id"]
#     #         if pid not in existing_ids:
#     #             new_rows.append(row)
#     #             existing_ids.add(pid)   # update immediately
#     #
#     #     if not new_rows:
#     #         logger.info("Batch skipped (all duplicates)")
#     #         continue
#     # #
#     #     # -----------------------------------------
#     #     # 8. Convert to Arrow
#     #     # -----------------------------------------
#     #     pa_table, row_errors = process_chunk(new_rows, arrow_schema)
#     #
#     #     if pa_table.num_rows == 0:
#     #         continue
#     #
#     #     # -----------------------------------------
#     #     # 9. Append
#     #     # -----------------------------------------
#     #     table.append(pa_table)
#     #     total_rows_inserted += pa_table.num_rows
#     #
#     #     logger.info(f"Inserted {pa_table.num_rows} rows")
#     # #
#     #     # 🔥 Important: free memory
#     #     del batch_records
#     #     del cleaned
#     #     del new_rows
#     #     del pa_table
#
#     # -----------------------------------------
#     # Final Response
#     # -----------------------------------------
#     return {
#         "success": True,
#         "files_processed": total_files,
#         # "records_fetched": total_rows_fetched,
#         # "records_inserted": total_rows_inserted,
#         "time_taken_sec": round(time.time() - start_time, 2)
#     }
# if __name__ == "__main__":
#     rr = insert_transaction_between_range()
#     print(rr['success'])

#########

def extract_rows_from_json(raw_data):
    """
    Handles:
    - List of dict  → already row format
    - {"data": [...]} → extract list
    - Column format  → convert to rows
    """
    print(f"raw_data: {raw_data}")
    print(f"raw_data: {raw_data}")
    # Case 1: Already row format
    if isinstance(raw_data, list):
        return raw_data

    # Case 2: Nested list inside dict
    if isinstance(raw_data, dict):

        if "data" in raw_data and isinstance(raw_data["data"], list):
            return raw_data["data"]

        # Column-based JSON
        return convert_column_json_to_rows(raw_data)

    return []

def convert_column_json_to_rows(data: dict) -> list:
    """
    Convert column-oriented JSON into row-oriented list of dict.
    Handles:
    - [[...]]
    - [...]
    - scalar values
    """


    if not isinstance(data, dict):
        return []

    cleaned = {}

    for key, value in data.items():

        # unwrap [[...]]
        if isinstance(value, list) and len(value) == 1 and isinstance(value[0], list):
            cleaned[key] = value[0]

        elif isinstance(value, list):
            cleaned[key] = value

        else:
            cleaned[key] = value

    list_lengths = [
        len(v) for v in cleaned.values()
        if isinstance(v, list)
    ]

    if not list_lengths:
        return []

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
        print("all_data",rows)
    return rows

from core.r2_client import get_r2_client
import json

def insert_transaction_between_range():

    namespace = "POS_Transactions"
    table_name = "Transaction_vars_2026-02-13"
    bucket = "pos-transaction-imei"
    prefix = "history/2026/01/29/"
    batch_size = 100

    start_time = time.time()

    # -----------------------------
    # 1. Load Iceberg Table
    # -----------------------------
    catalog = get_catalog_client()
    table = catalog.load_table(f"{namespace}.{table_name}")
    arrow_schema = table.schema().as_arrow()

    # -----------------------------
    # 2. Load existing pri_ids
    # -----------------------------
    # try:
    #     existing_ids = set(
    #         table.scan(selected_fields=["pri_id"])
    #              .to_arrow()["pri_id"]
    #              .to_pylist()
    #     )
    # except Exception:
    #     existing_ids = set()

    # -----------------------------
    # 3. List R2 Files
    # -----------------------------
    keys = list_json_files(prefix=prefix, bucket=bucket)
    if not keys:
        return {"success": True, "message": "No files found"}

    rows = []
    seen_ids = set()
    files_processed = 0

    for key in keys[:batch_size]:

        obj = get_r2_client().get_object(Bucket=bucket, Key=key)
        raw_data = json.loads(obj["Body"].read())

        # print("raw_data type:", type(raw_data))
        if isinstance(raw_data, dict):
            pri_id = raw_data.get("pri_id")

            if pri_id and pri_id not in seen_ids:
                rows.append(raw_data)
                seen_ids.add(pri_id)
        files_processed += 1


    if not rows:
        return {
            "success": True,
            "message": "No new records found",
            "time_taken_sec": round(time.time() - start_time, 2)
        }

    # -----------------------------
    # 4. Clean Rows
    # -----------------------------
    rows = clean_rows(
        rows=rows,
        boolean_fields=BOOLEAN_FIELDS,
        timestamps_fields=TIMESTAMP_FIELDS,
        field_overrides=FIELD_OVERRIDES
    )

    # -----------------------------
    # 5. Convert to Arrow
    # -----------------------------
    print("rows",rows)
    print("arrow_schema",arrow_schema)
    arrow_table, conversion_errors = process_chunk(rows, arrow_schema)
    # print("arrow_table:",arrow_table["pri_id"])
    # for r in arrow_table["pri_id"]:
    #     print("data:",r)
    if arrow_table and arrow_table.num_rows > 0:
        table.append(arrow_table)

    return {
        "success": True,
        "files_processed": files_processed,
        "rows_inserted": arrow_table.num_rows,
        "time_taken_sec": round(time.time() - start_time, 2)
    }

dd = insert_transaction_between_range()
print(dd)