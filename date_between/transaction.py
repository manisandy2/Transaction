from fastapi import APIRouter, Query, Body, HTTPException
import time
from .utility import schema,clean_rows
from .transaction_utility import (REQUIRED_FIELDS, FIELD_OVERRIDES,
                                  VARCHAR_FIELDS, TIMESTAMP_FIELDS, BOOLEAN_FIELDS)
from core.catalog_client import *
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyiceberg.catalog import NoSuchTableError
from core.mysql_client import MysqlCatalog
from core.logger import get_logger
from .table_utility import process_chunk
from .error_handler import handle_ingestion_error

url_prefix = "Transaction"

logger = get_logger("Transaction")

router = APIRouter(prefix=f"/{url_prefix}", tags=["Transaction"])

# mysql | range | chunk_size | multithreading | arrow | append
@router.post("/ingest/mysql-range")
def insert_transaction_between_range(
        start_range: int = Query(0, description="Start row offset for MySQL data fetch"),
        end_range: int = Query(100, description="End row offset for MySQL data fetch"),
        chunk_size: int = Query(10000, description="Chunk size for processing"),
):
    total_start = time.time()
    namespace, table_name = "POS_Transactions", "Transaction_vars"
    dbname = f"{url_prefix}"

    logger.info(
        f"START ingestion | table={namespace}.{table_name} "
        f"range=({start_range},{end_range}) chunk_size={chunk_size}"
    )

    mysql_creds = MysqlCatalog()

    # 1. Load Iceberg Table
    try:
        catalog = get_catalog_client()
        table_identifier = f"{namespace}.{table_name}"
        tbl = catalog.load_table(table_identifier)
        logger.info(f"Iceberg table loaded successfully")
    except NoSuchTableError:
        logger.error("Iceberg table not found")
        raise HTTPException(status_code=404, detail=f"Table not found")
    except Exception as e:
        logger.exception("Iceberg table load failed")
        raise HTTPException(status_code=500, detail=str(e))

    # 2. Iterate in Batches
    current_start = start_range
    batch_index = 0
    total_rows_fetched = 0
    total_rows_successful = 0
    failed_batches = []
    
    execution_stats = {
        "fetch_time": 0.0,
        "clean_time": 0.0,
        "arrow_time": 0.0,
        "append_time": 0.0
    }

    try:
        while current_start < end_range:
            batch_index += 1
            current_end = min(current_start + chunk_size, end_range)
            logger.info(f"Processing batch {batch_index}: range({current_start}, {current_end})")

            # --- FETCH ---
            t0 = time.time()
            rows = []
            try:
                rows = mysql_creds.get_range(dbname, current_start, current_end)
                
                rows_count = len(rows)
                total_rows_fetched += rows_count
                execution_stats["fetch_time"] += (time.time() - t0)
                
                if not rows:
                    if batch_index == 1:
                        logger.warning(f"No rows found for initial range {current_start}-{current_end}")
                    else:
                        logger.info(f"No rows found for batch {batch_index}, continuing...")
                    current_start += chunk_size
                    continue

                logger.debug(f"Batch {batch_index} fetched {rows_count} rows")

            except Exception as e:
                logger.exception(f"Batch {batch_index} MySQL fetch failed")
                failed_batches.append({
                    "batch_index": batch_index,
                    "range": (current_start, current_end),
                    "error": f"MySQL Fetch Error: {str(e)}",
                    "stage": "FETCH"
                })
                handle_ingestion_error(
                    table_name=table_name,
                    failed_records=[], # No records to save if fetch failed
                    error_type="FETCH_ERROR",
                    error_message=f"Batch {batch_index} MySQL fetch failed: {str(e)}",
                    namespace=namespace
                )
                current_start += chunk_size
                continue

            # --- CLEAN ---
            t1 = time.time()
            try:
                rows, row_errors = clean_rows(
                    rows=rows,
                    boolean_fields=BOOLEAN_FIELDS,
                    timestamps_fields=TIMESTAMP_FIELDS,
                    field_overrides=FIELD_OVERRIDES
                )
                execution_stats["clean_time"] += (time.time() - t1)
                
                if row_errors:
                    logger.warning(f"Batch {batch_index} had {len(row_errors)} row cleaning errors")
                    # Extract rows that had errors if possible, or just store the error info
                    handle_ingestion_error(
                        table_name=table_name,
                        failed_records=row_errors,
                        error_type="CLEANING_ERROR",
                        error_message=f"Batch {batch_index} row cleaning errors",
                        namespace=namespace
                    )
            except Exception as e:
                logger.exception(f"Batch {batch_index} cleaning failed")
                failed_batches.append({
                    "batch_index": batch_index,
                    "range": (current_start, current_end),
                    "error": f"Cleaning Error: {str(e)}",
                    "stage": "CLEAN"
                })
                handle_ingestion_error(
                    table_name=table_name,
                    failed_records=rows,
                    error_type="BATCH_CLEAN_ERROR",
                    error_message=f"Batch {batch_index} cleaning failed: {str(e)}",
                    namespace=namespace
                )
                current_start += chunk_size
                continue

            # --- CONVERT ---
            t2 = time.time()
            try:
                # Infer schema from check first row of this batch
                iceberg_schema_obj, arrow_schema_obj = schema(
                    rows[0],
                    required_fields=REQUIRED_FIELDS,
                    field_overrides=FIELD_OVERRIDES
                )
                
                arrow_table, conversion_errors = process_chunk(rows, arrow_schema_obj)
                
                if conversion_errors:
                    logger.warning(f"Batch {batch_index} had {len(conversion_errors)} arrow conversion errors")
                    handle_ingestion_error(
                        table_name=table_name,
                        failed_records=conversion_errors,
                        error_type="CONVERSION_ERROR",
                        error_message=f"Batch {batch_index} arrow conversion errors",
                        namespace=namespace
                    )

                execution_stats["arrow_time"] += (time.time() - t2)
            except Exception as e:
                logger.exception(f"Batch {batch_index} Arrow conversion failed")
                failed_batches.append({
                    "batch_index": batch_index,
                    "range": (current_start, current_end),
                    "error": f"Arrow Conversion Error: {str(e)}",
                    "stage": "CONVERT"
                })
                handle_ingestion_error(
                    table_name=table_name,
                    failed_records=rows,
                    error_type="BATCH_CONVERT_ERROR",
                    error_message=f"Batch {batch_index} Arrow conversion failed: {str(e)}",
                    namespace=namespace
                )
                current_start += chunk_size
                continue

            # --- APPEND ---
            t3 = time.time()
            try:
                tbl.append(arrow_table)
                execution_stats["append_time"] += (time.time() - t3)
                total_rows_successful += arrow_table.num_rows
                logger.info(f"Batch {batch_index} appended successfully ({arrow_table.num_rows} rows)")
            except Exception as e:
                logger.error(f"Batch {batch_index} append failed: {e}")
                failed_batches.append({
                    "batch_index": batch_index,
                    "range": (current_start, current_end),
                    "error": f"Iceberg Append Error: {str(e)}",
                    "stage": "APPEND"
                })
                handle_ingestion_error(
                    table_name=table_name,
                    failed_records=rows,
                    error_type="BATCH_APPEND_ERROR",
                    error_message=f"Batch {batch_index} Iceberg append failed: {str(e)}",
                    namespace=namespace
                )
            
            # Move to next batch
            current_start += chunk_size

    except Exception as e:
        logger.exception("Critical error during ingestion loop")
        handle_ingestion_error(
            table_name=table_name,
            failed_records=[],
            error_type="CRITICAL_INGESTION_ERROR",
            error_message=f"Ingestion crashed: {str(e)}",
            namespace=namespace
        )
        raise HTTPException(status_code=500, detail=f"Ingestion crashed: {str(e)}")

    total_end = time.time()
    
    # Handle failed batches summary logging
    if failed_batches:
        logger.warning(f"{len(failed_batches)} batches failed during ingestion")
        failed_records_summary = [
            {"batch": b["batch_index"], "stage": b["stage"], "error": b["error"]} 
            for b in failed_batches
        ]
        logger.info(f"Failed batches summary: {failed_records_summary}")

    logger.info(
        f"END ingestion | fetched={total_rows_fetched} successful={total_rows_successful} "
        f"failed_batches={len(failed_batches)} total_time={total_end - total_start:.2f}s"
    )

    response = {
        "success": True,
        "message": "Data ingestion completed (batched)",
        "rows_fetched": total_rows_fetched,
        "rows_successful": total_rows_successful,
        "total_batches_processed": batch_index,
        "batches_failed": len(failed_batches),
        "execution_stats": {k: round(v, 2) for k, v in execution_stats.items()},
        "total_time": round(total_end - total_start, 2),
    }

    if failed_batches:
        response["failed_batches_summary"] = [
            {"batch": b["batch_index"], "stage": b["stage"], "error": b["error"]}
            for b in failed_batches
        ]

    return response