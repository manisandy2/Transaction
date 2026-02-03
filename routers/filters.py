from pyiceberg.catalog import NoSuchTableError
from core.catalog_client import get_catalog_client
from fastapi import APIRouter,HTTPException,Query
from pyiceberg.expressions import And, EqualTo
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import numpy as np
import datetime
import pyarrow.compute as pc

router = APIRouter(prefix="", tags=["filters"])

# @router.get("/filters/get")
# def filter_customer_phone(
#     namespace: str = Query("pos_transactions01", description="Iceberg namespace name"),
#     table_name: str = Query("transaction01", description="Iceberg table name"),
#     customer_mobile: str | None = Query(None, description="Filter by customer_mobile__c")
# ):
#     import datetime
#     """
#     Inspect an existing Iceberg table's metadata.
#     Optionally filter by partition values (bill_date, store_code, customer_mobile).
#     Adds a timeline field to measure total execution time.
#     """
#     start_time = time.perf_counter()  # Start timeline measurement
#
#     table_identifier = f"{namespace}.{table_name}"
#     catalog = get_catalog_client()
#
#     # --- Load the table ---
#     try:
#         tbl = catalog.load_table(table_identifier)
#     except NoSuchTableError:
#         raise HTTPException(status_code=404, detail=f"Table not found: {table_identifier}")
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Error loading table: {str(e)}")
#
#     # --- Build filter expressions dynamically ---
#     expr = None
#     if customer_mobile:
#         try:
#             cond = EqualTo("customer_mobile__c", int(customer_mobile))
#         except:
#             raise HTTPException(status_code=400, detail=f"Invalid filter value: {str(e)}")
#         expr = cond
#     # --- Perform scan ---
#     try:
#         scan = tbl.scan(row_filter=expr) if expr else tbl.scan()
#         df = scan.to_arrow().to_pandas()
#         df = df.replace({np.nan: None})
#         # df = arrow_table.to_pandas().reset_index(drop=True)
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Error reading data: {str(e)}")
#
#     timeline = round(time.perf_counter() - start_time, 3)  # seconds (rounded to 3 decimals)
#
#     # --- Construct response ---
#     return {
#         "namespace": namespace,
#         "table_name": table_name,
#         "customer_mobile": customer_mobile,
#         "count": len(df),
#         "sample_rows": df.head(10).to_dict(orient="records"),
#         "timeline_seconds": timeline
#     }

@router.get("/filters/get-count")
def filter_customer_phone(
    namespace: str = Query("pos_transactions01"),
    table_name: str = Query("transaction01"),
    customer_mobile: int = Query(..., description="customer_mobile__c"),
):
    import time
    start_time = time.perf_counter()

    table_identifier = f"{namespace}.{table_name}"
    catalog = get_catalog_client()

    # --- Load table ---
    try:
        tbl = catalog.load_table(table_identifier)
    except NoSuchTableError:
        raise HTTPException(status_code=404, detail="Table not found")

    # --- Iceberg scan (only required filter) ---
    try:
        # Full data scan
        scan = tbl.scan(
            row_filter=EqualTo("customer_mobile__c", customer_mobile)
        )
        df = scan.to_arrow().to_pandas()
        df = df.replace({np.nan: None})
        
        count = len(df)
        data = df.to_dict(orient="records")

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return {
        "namespace": namespace,
        "table_name": table_name,
        "customer_mobile": customer_mobile,
        "count": count,
        "data": data,
        "timeline_seconds": round(time.perf_counter() - start_time, 4),
    }


@router.get("/filters/get")
def filter_customer_phones_mysql(
    namespace: str = Query("pos_transactions01", description="Iceberg namespace name"),
    table_name: str = Query("transaction01", description="Iceberg table name"),
    phone: str = Query(None, description="Filter by customer_mobile__c"),
):
    import datetime
    start_time = time.perf_counter()

    # Iceberg table identifier
    table_identifier = f"{namespace}.{table_name}"

    catalog = get_catalog_client()

    # --- Load the table ---
    try:
        tbl = catalog.load_table(table_identifier)
    except NoSuchTableError:
        raise HTTPException(status_code=404, detail=f"Table not found: {table_identifier}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error loading table: {str(e)}")

    # --- Build Iceberg Filter ---
    expr = None
    if phone:
        try:
            expr = EqualTo("customer_mobile__c", int(phone))
        except:
            raise HTTPException(status_code=400, detail="Invalid phone number")

    # --- Perform scan ---
    try:
        scan = tbl.scan(row_filter=expr) if expr else tbl.scan()
        df = scan.to_arrow().to_pandas()
        df = df.replace({np.nan: None})
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading data: {str(e)}")

    timeline = round(time.perf_counter() - start_time, 3)

    # --- Response ---
    return {
        "namespace": namespace,
        "table_name": table_name,
        "phone": phone,
        "count": len(df),
        "sample_rows": df.head(10).to_dict(orient="records"),
        "timeline_seconds": timeline
    }

@router.get("/filters/exact-date")
def filter_exact_date(
    namespace: str = Query("pos_transactions01", description="Iceberg namespace name"),
    table_name: str = Query("transaction01", description="Iceberg table name"),
    bill_date: str | None = Query(None, description="Filter by Bill_Date__c (YYYY-MM-DD)"),
    customer_mobile: int | None = Query(None, description="Filter by customer_mobile__c")
):
    import datetime
    """
    Inspect an existing Iceberg table's metadata.
    Optionally filter by partition values (bill_date, store_code, customer_mobile).
    Adds a timeline field to measure total execution time.
    """
    start_time = time.perf_counter()  # Start timeline measurement

    table_identifier = f"{namespace}.{table_name}"
    catalog = get_catalog_client()

    # --- Load the table ---
    try:
        tbl = catalog.load_table(table_identifier)
    except NoSuchTableError:
        raise HTTPException(status_code=404, detail=f"Table not found: {table_identifier}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error loading table: {str(e)}")

    # --- Build filter expressions dynamically ---
    expr = None
    try:
        if bill_date:
            bill_date_parsed = datetime.datetime.strptime(bill_date, "%Y-%m-%d").date()
            expr = EqualTo("Bill_Date__c", bill_date_parsed)

        if customer_mobile:
            cond = EqualTo("customer_mobile__c", int(customer_mobile))
            expr = cond if expr is None else And(expr, cond)

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid filter value: {str(e)}")

    # --- Perform scan ---
    try:
        scan = tbl.scan(row_filter=expr) if expr else tbl.scan()
        arrow_table = scan.to_arrow()
        df = arrow_table.to_pandas().reset_index(drop=True)
        df = df.replace({np.nan: None})
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading data: {str(e)}")

    end_time = time.perf_counter()  # End timeline measurement
    total_time = round(end_time - start_time, 3)  # seconds (rounded to 3 decimals)

    # --- Construct response ---
    return {
        "namespace": namespace,
        "table_name": table_name,
        "filter_applied": {
            "bill_date": bill_date,
            "customer_mobile": customer_mobile
        },
        "count": len(df),
        "sample_rows": df.head(10).to_dict(orient="records"),
        "timeline_seconds": total_time
    }

@router.get("/filters/date-range")
def filter_between_date_range(
    namespace: str = Query("pos_transactions"),
    table_name: str = Query("iceberg_with_partitioning"),
    start_date: str = Query(..., description="YYYY-MM-DD"),
    end_date: str = Query(..., description="YYYY-MM-DD"),
    phone: str | None = Query(None, description="Filter by customer_mobile__c")
):
    from pyiceberg.expressions import And, GreaterThanOrEqual, LessThanOrEqual, EqualTo
    import datetime
    start = time.perf_counter()

    # validate dates
    try:
        d1 = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()
        d2 = datetime.datetime.strptime(end_date, "%Y-%m-%d").date()
    except:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")

    table_identifier = f"{namespace}.{table_name}"
    catalog = get_catalog_client()

    try:
        tbl = catalog.load_table(table_identifier)
    except NoSuchTableError:
        raise HTTPException(status_code=404, detail=f"Table not found: {table_identifier}")

    # base expr = date range
    expr = And(
        GreaterThanOrEqual("Bill_Date__c", d1),
        LessThanOrEqual("Bill_Date__c", d2),
    )

    # add phone filter if present
    if phone:
        try:
            phone_int = int(phone)
        except:
            raise HTTPException(status_code=400, detail="phone must be integer digits")
        expr = And(expr, EqualTo("customer_mobile__c", phone_int))

    # scan / read data
    try:
        df = tbl.scan(row_filter=expr).to_arrow().to_pandas().reset_index(drop=True)
        df = df.replace({np.nan: None})
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading data: {str(e)}")

    return {
        "namespace": namespace,
        "table_name": table_name,
        "start_date": start_date,
        "end_date": end_date,
        "phone": phone,
        "count": len(df),
        "sample_rows": df.head(10).to_dict(orient="records"),
        "timeline_seconds": round(time.perf_counter() - start, 3)
    }

@router.get("/filters/pri_id")
def filter_id(
    namespace: str = Query("POS_transactions"),
    table_name: str = Query("Transaction"),
    pri_id: str = Query(default=None),

):

    start_time = time.perf_counter()
    table_identifier = f"{namespace}.{table_name}"
    catalog = get_catalog_client()

    try:
        tbl = catalog.load_table(table_identifier)
    except NoSuchTableError:
        raise HTTPException(status_code=404, detail=f"Table not found: {table_identifier}")

    if pri_id is None:
        raise HTTPException(status_code=400, detail="pri_id is required")

    try:
        pri_id_value = int(pri_id)
    except:
        raise HTTPException(status_code=400, detail="pri_id must be integer")

    expr = EqualTo("pri_id", pri_id_value)

    try:
        df = tbl.scan(row_filter=expr).to_arrow().to_pandas().reset_index(drop=True)
        df = df.replace({np.nan: None})
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading data: {str(e)}")

    total_time = round(time.perf_counter() - start_time, 3)

    return {
        "namespace": namespace,
        "table_name": table_name,
        "count": len(df),
        "sample_rows": df.head(10).to_dict(orient="records"),
        "timeline_seconds": total_time
    }

# @router.get("/filters/get-multi")
# def filter_customer_phone_multi(
#     namespaces: list[str] = Query(["pos_transactions01", "pos_transactions02", "pos_transactions03", "pos_transactions04"], description="List of Iceberg namespaces"),
#     table_names: list[str] = Query(["transaction01", "transaction02", "transaction03", "transaction04"], description="List of Iceberg table names (same order as namespaces)"),
#     customer_mobile: str | None = Query(None, description="Filter by customer_mobile__c"),
# ):
#     """
#     Fetch records from multiple Iceberg tables (across namespaces)
#     using a single filter condition (e.g., customer_mobile__c).
#     Returns individual table metrics + total execution summary.
#     """
#
#     import time
#     import pandas as pd
#     from pyiceberg.expressions import EqualTo
#     from fastapi import HTTPException
#
#     catalog = get_catalog_client()
#     all_results = []
#     total_start = time.perf_counter()
#
#     # --- Iterate over all namespace-table pairs ---
#     for idx, (ns, tbl_name) in enumerate(zip(namespaces, table_names)):
#         table_identifier = f"{ns}.{tbl_name}"
#         start_time = time.perf_counter()
#
#         try:
#             tbl = catalog.load_table(table_identifier)
#         except Exception as e:
#             all_results.append({
#                 "namespace": ns,
#                 "table_name": tbl_name,
#                 "error": str(e),
#                 "count": 0,
#                 "timeline_seconds": 0
#             })
#             continue
#
#         # Build filter condition
#         expr = None
#         if customer_mobile:
#             try:
#                 expr = EqualTo("customer_mobile__c", int(customer_mobile))
#             except Exception as e:
#                 raise HTTPException(status_code=400, detail=f"Invalid filter value: {str(e)}")
#
#         # Perform scan
#         try:
#             scan = tbl.scan(row_filter=expr) if expr else tbl.scan()
#             df = scan.to_arrow().to_pandas()
#         except Exception as e:
#             all_results.append({
#                 "namespace": ns,
#                 "table_name": tbl_name,
#                 "error": f"Error reading table: {str(e)}",
#                 "count": 0,
#                 "timeline_seconds": 0
#             })
#             continue
#
#         # Measure timing
#         timeline = round(time.perf_counter() - start_time, 3)
#
#         all_results.append({
#             "namespace": ns,
#             "table_name": tbl_name,
#             "record_count": len(df),
#             "timeline_seconds": timeline,
#             "sample_rows": df.head(3).to_dict(orient="records"),
#         })
#
#     total_time = round(time.perf_counter() - total_start, 3)
#
#     # Combine results
#     summary = {
#         "total_namespaces": len(namespaces),
#         "total_tables": len(table_names),
#         "total_execution_time": total_time,
#         "details": all_results
#     }
#
#     return summary

def process_table(namespace: str, table_name: str, customer_mobile: str | None):
    """Worker function for each table query (runs in parallel threads)."""
    start_time = time.perf_counter()
    catalog = get_catalog_client()
    table_identifier = f"{namespace}.{table_name}"

    result = {
        "namespace": namespace,
        "table_name": table_name,
        "record_count": 0,
        "timeline_seconds": 0,
        "sample_rows": [],
        "error": None
    }

    try:
        tbl = catalog.load_table(table_identifier)
    except NoSuchTableError:
        result["error"] = f"Table not found: {table_identifier}"
        return result
    except Exception as e:
        result["error"] = f"Error loading table: {str(e)}"
        return result

    # 🔸 Build filter condition
    expr = None
    if customer_mobile:
        try:
            expr = EqualTo("customer_mobile__c", int(customer_mobile))
        except Exception as e:
            result["error"] = f"Invalid filter value: {str(e)}"
            return result

    # 🔸 Perform table scan
    try:
        scan = tbl.scan(row_filter=expr) if expr else tbl.scan()
        df = scan.to_arrow().to_pandas()
        df = df.replace({np.nan: None})
        result["record_count"] = len(df)
        result["sample_rows"] = df.head(3).to_dict(orient="records")
    except Exception as e:
        result["error"] = f"Error reading table: {str(e)}"
        return result

    result["timeline_seconds"] = round(time.perf_counter() - start_time, 3)
    return result

@router.get("/filters/get-multi")
def filter_customer_phone_multi(
    namespaces: list[str] = Query(["pos_transactions01", "pos_transactions02", "pos_transactions03", "pos_transactions04"], description="List of Iceberg namespaces"),
    table_names: list[str] = Query(["transaction01", "transaction02", "transaction03", "transaction04"], description="List of Iceberg table names (same order as namespaces)"),
    customer_mobile: str | None = Query(None, description="Filter by customer_mobile__c"),
    max_threads: int = Query(4, description="Maximum number of parallel threads"),
):
    """
    Multithreaded filter across multiple Iceberg namespaces and tables.
    Executes all queries concurrently and returns per-table metrics.
    """

    total_start = time.perf_counter()
    all_results = []

    # --- Use ThreadPoolExecutor for parallel querying ---
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = [
            executor.submit(process_table, ns, tbl_name, customer_mobile)
            for ns, tbl_name in zip(namespaces, table_names)
        ]

        # Collect completed results
        for future in as_completed(futures):
            all_results.append(future.result())

    total_time = round(time.perf_counter() - total_start, 3)

    return {
        "total_namespaces": len(namespaces),
        "total_tables": len(table_names),
        "thread_count": max_threads,
        "total_execution_time": total_time,
        "details": all_results
    }

@router.get("/filters/ph-count")
def get_phone_transaction_count(
    namespace: str = Query("pos_transactions01", description="Iceberg namespace name"),
    table_name: str = Query("transaction01", description="Iceberg table name"),
    phone: str = Query(None, description="Filter by customer_mobile__c"),
):
    import datetime
    start_time = time.perf_counter()
    print(phone)

    # Iceberg table identifier
    table_identifier = f"{namespace}.{table_name}"

    catalog = get_catalog_client()

    # --- Load the table ---
    try:
        tbl = catalog.load_table(table_identifier)
    except NoSuchTableError:
        raise HTTPException(status_code=404, detail=f"Table not found: {table_identifier}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error loading table: {str(e)}")

    # --- Build Iceberg Filter ---
    expr = None
    if phone:
        try:
            expr = EqualTo("customer_mobile__c", int(phone))
        except:
            raise HTTPException(status_code=400, detail="Invalid phone number")

    # --- Perform scan ---
    print(expr)
    try:
        scan = tbl.scan(row_filter=expr).count()
        print(scan)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading data: {str(e)}")

    timeline = round(time.perf_counter() - start_time, 3)

    # --- Response ---
    return {
        "namespace": namespace,
        "table_name": table_name,
        "phone": phone,
        "count": scan,
        "timeline_seconds": timeline
    }

@router.get("/pri-id/last")
def get_last_pri_id(
    namespace: str = Query("POS_transactions"),
    table_name: str = Query("Transaction"),
    column: str = Query("pri_id", description="Numeric column to get max value")
):
    start_time = time.perf_counter()
    table_identifier = f"{namespace}.{table_name}"

    try:
        catalog = get_catalog_client()
        table = catalog.load_table(table_identifier)
    except NoSuchTableError:
        raise HTTPException(
            status_code=404,
            detail=f"Table not found: {table_identifier}"
        )

    # ✅ Validate column exists
    schema_fields = {f.name for f in table.schema().fields}
    if column not in schema_fields:
        raise HTTPException(
            status_code=400,
            detail=f"Column '{column}' not found in table schema"
        )

    try:
        arrow_table = table.scan(
            selected_fields=[column]
        ).to_arrow()

        if arrow_table.num_rows == 0:
            return {
                "namespace": namespace,
                "table_name": table_name,
                "column": column,
                "last_value": None,
                "timeline_seconds": round(time.perf_counter() - start_time, 3)
            }

        # 🔥 Fast Arrow aggregation (NO pandas)
        last_value = pc.max(arrow_table[column]).as_py()

        return {
            "namespace": namespace,
            "table_name": table_name,
            "column": column,
            "last_value": last_value,
            "timeline_seconds": round(time.perf_counter() - start_time, 3)
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get last value for column '{column}': {str(e)}"
        )
from datetime import datetime, date
import pandas as pd
import math

def normalize_datetime(val):
    if val is None:
        return None

    # NaN from pandas
    if isinstance(val, float) and math.isnan(val):
        return None

    # Pandas Timestamp → datetime
    if isinstance(val, pd.Timestamp):
        return val.to_pydatetime()

    # date → datetime (midnight)
    if isinstance(val, date) and not isinstance(val, datetime):
        return datetime.combine(val, datetime.min.time())

    return val

@router.get("/last-date")
def get_last_date_value(
    namespace: str = Query("POS_transactions"),
    table: str = Query("Transaction"),
    column: str = Query("created_at")
):
    """
    Fetch the latest (MAX) date/timestamp from an Iceberg table
    """
    try:
        catalog = get_catalog_client()
        table_identifier = f"{namespace}.{table}"
        iceberg_table = catalog.load_table(table_identifier)

        scan = (
            iceberg_table.scan(
                selected_fields=[column]
            )
            .to_arrow()
        )

        if scan.num_rows == 0:
            return {
                "namespace": namespace,
                "table": table,
                "column": column,
                "last_value": None
            }

        df = scan.to_pandas()
        df[column] = pd.to_datetime(df[column], errors="coerce")
        # raw_last_value = df[column].max()
        df = df.dropna(subset=[column])

        # last_value = normalize_datetime(raw_last_value)
        if df.empty:
            return {
                "namespace": namespace,
                "table": table,
                "column": column,
                "last_value": None
            }

        last_value = df[column].max()
        return {
            "namespace": namespace,
            "table": table,
            "column": column,
            "last_value": last_value.isoformat()
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch last date value: {str(e)}"
        )