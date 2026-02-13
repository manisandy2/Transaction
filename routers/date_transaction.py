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
from datetime import datetime

url_prefix = "Transaction-date-between"

logger = get_logger("Transaction")

router = APIRouter(prefix=f"/{url_prefix}", tags=["Transaction"])


@router.post("/ingest/mysql-date-range")
def insert_transaction_between_date_range(
    start_date: datetime = Query(..., description="Start datetime (YYYY-MM-DD HH:MM:SS)"),
    end_date: datetime = Query(..., description="End datetime (YYYY-MM-DD HH:MM:SS)"),
    chunk_size: int = Query(10000, description="Chunk size for processing"),
):
    total_start = time.time()
    namespace, table_name = "POS_Transactions", "Transaction_vars"
    dbname = "Transaction"

    # -------------------------
    # 1️⃣ Validate Date Range
    # -------------------------
    if start_date > end_date:
        raise HTTPException(
            status_code=400,
            detail="start_date must be less than or equal to end_date"
        )

    logger.info(
        f"START ingestion | table={namespace}.{table_name} "
        f"date_range=({start_date},{end_date}) chunk_size={chunk_size}"
    )

    mysql_creds = MysqlCatalog()

    # -------------------------
    # 2️⃣ Load Iceberg Table
    # -------------------------
    try:
        catalog = get_catalog_client()
        tbl = catalog.load_table(f"{namespace}.{table_name}")
        logger.info("Iceberg table loaded successfully")
    except NoSuchTableError:
        raise HTTPException(status_code=404, detail="Table not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    total_rows_fetched = 0
    total_rows_successful = 0
    failed_batches = []
    batch_index = 0
    offset = 0

    try:
        while True:
            batch_index += 1

            # -------------------------
            # 3️⃣ Fetch from MySQL
            # -------------------------
            rows = mysql_creds.get_date_range(
                dbname=dbname,
                start_date=start_date,
                end_date=end_date,
                offset=offset,
                limit=chunk_size
            )

            if not rows:
                break
            print("count:",len(rows))
            time.sleep(30)
            total_rows_fetched += len(rows)

            # -------------------------
            # 4️⃣ Clean Rows
            # -------------------------
            rows, row_errors = clean_rows(
                rows=rows,
                boolean_fields=BOOLEAN_FIELDS,
                timestamps_fields=TIMESTAMP_FIELDS,
                field_overrides=FIELD_OVERRIDES
            )

            if row_errors:
                handle_ingestion_error(
                    table_name=table_name,
                    failed_records=row_errors,
                    error_type="CLEANING_ERROR",
                    error_message="Row cleaning error",
                    namespace=namespace
                )

            # -------------------------
            # 5️⃣ Convert to Arrow
            # -------------------------
            iceberg_schema_obj, arrow_schema_obj = schema(
                rows[0],
                required_fields=REQUIRED_FIELDS,
                field_overrides=FIELD_OVERRIDES
            )

            arrow_table, conversion_errors = process_chunk(rows, arrow_schema_obj)

            if conversion_errors:
                handle_ingestion_error(
                    table_name=table_name,
                    failed_records=conversion_errors,
                    error_type="CONVERSION_ERROR",
                    error_message="Arrow conversion error",
                    namespace=namespace
                )

            # -------------------------
            # 6️⃣ Append to Iceberg
            # -------------------------
            tbl.append(arrow_table)

            total_rows_successful += arrow_table.num_rows
            logger.info(
                f"Batch {batch_index} appended successfully "
                f"({arrow_table.num_rows} rows)"
            )

            offset += chunk_size

    except Exception as e:
        logger.exception("Ingestion failed")
        raise HTTPException(status_code=500, detail=str(e))

    total_time = round(time.time() - total_start, 2)

    logger.info(
        f"END ingestion | fetched={total_rows_fetched} "
        f"successful={total_rows_successful} "
        f"time={total_time}s"
    )

    return {
        "success": True,
        "rows_fetched": total_rows_fetched,
        "rows_successful": total_rows_successful,
        "total_batches": batch_index,
        "total_time_sec": total_time
    }