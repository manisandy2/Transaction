from fastapi import FastAPI, APIRouter, Query, HTTPException,Body
from packaging.metadata import Metadata
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema, NestedField
from pyiceberg.types import StringType, LongType, DateType,TimestampType
import time, json, boto3, os
import io
from botocore.client import Config
import logging
import time
from ...mysql_creds import *
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform
from datetime import datetime
from ...mapping import *
from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError
from sqlalchemy.sql.sqltypes import NullType
from ...core.r2_client import get_r2_client
import pandas as pd
from ...core.catalog_client import get_catalog_client
from fastapi import APIRouter,Query,HTTPException

app = FastAPI()
router = APIRouter(prefix="/transaction", tags=["transaction bucket data store"])
logger = logging.getLogger(__name__)


R2_BUCKET_NAME = os.getenv("BUCKET_NAME")


s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("ENDPOINT"),
    aws_access_key_id=os.getenv("ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("SECRET_ACCESS_KEY"),
    config=Config(signature_version="s3v4"),
    region_name="auto"
)


def store_json_to_r2(data, key, metadata):
    """Upload JSON (dict or list) with custom metadata."""
    try:
        # --- Ensure data is serializable ---
        if isinstance(data, (dict, list, int, float, str)):
            body = json.dumps(data, indent=2, ensure_ascii=False)
        else:
            raise TypeError(f"Unsupported data type for R2 upload: {type(data)}")

        # --- Prepare metadata ---
        safe_metadata = {}
        if isinstance(metadata, dict):
            for k, v in metadata.items():
                safe_metadata[str(k).lower().replace("_", "-")] = str(v)
        else:
            print(f"⚠️ Metadata is not dict: {metadata}, skipping metadata.")

        # --- Upload to R2 ---
        get_r2_client().put_object(
            Bucket=R2_BUCKET_NAME,
            Key=key,
            Body=body.encode("utf-8"),   # ✅ body is always string now
            ContentType="application/json",
            Metadata=safe_metadata,
        )

        print(f"✅ Stored {key} ({len(body)} bytes)")

    except Exception as e:
        print(f"❌ Error storing {key}: {e}")
        raise

def fetch_json_from_r2(key: str):

    try:
        # get_r2_client().get_object()
        resp = get_r2_client().get_object(Bucket=R2_BUCKET_NAME, Key=key)
        body = resp["Body"].read()
        data = json.loads(body)
        return data
    except get_r2_client().exceptions.NoSuchKey:
        print(f"⚠️ R2 key not found: {key}")
        return None
    except Exception as e:
        print(f"⚠️ Error fetching key {key} from R2: {e}")
        return None


def make_json_serializable(record: dict) -> dict:
    """Convert all datetime objects in the record to ISO strings"""
    serializable = {}
    for k, v in record.items():
        if isinstance(v, datetime):
            serializable[k] = v.isoformat()  # e.g., "2020-06-25T17:37:36"
        else:
            serializable[k] = v
    return serializable

def safe_parse_date(value):
    """Flexible date parser for Bill_Date__c"""
    if isinstance(value, datetime):
        return value
    if not value:
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(value[:10], fmt)
        except Exception:
            continue
    return None


def load_r2_json(bucket: str, key: str):
    """Read JSON (list or dict) from R2 and normalize to list."""
    try:
        obj = get_r2_client().get_object(Bucket=bucket, Key=key)
        body = obj["Body"].read().decode("utf-8")
        loaded = json.loads(body)
        if isinstance(loaded, dict):
            return [loaded]
        elif isinstance(loaded, list):
            return loaded
        else:
            return []
    except get_r2_client().exceptions.NoSuchKey:
        return []
    except Exception as e:
        print(f"⚠️ Error loading {key}: {e}")
        return []


def list_r2_objects(prefix: str):
    print(f"Listing objects under prefix: {prefix}")
    try:

        response = get_r2_client().get_object(Bucket=R2_BUCKET_NAME, key=prefix)
        print("data", response["Body"].read())
        body = response["Body"].read()
        data = json.loads(body)
        return data

    except Exception as e:
        raise (Exception(f"R2 list error: {e}"))


# ============================================================
# 🚀 ROUTE: Store Range to R2 + MySQL
# ============================================================

# @router.post("/create")
# async def transaction(
#     start_range: int = Query(0, description="Start row (e.g. 0)"),
#     end_range: int = Query(100000, description="End row (e.g. 100000)")
# ):
#     if end_range <= start_range:
#         raise HTTPException(status_code=400, detail="end_range must be greater than start_range")
#
#     total_start = time.time()
#     mysql_creds = MysqlCatalog()  # <-- Your existing MySQL data fetcher class
#
#     dbname = "Transaction"
#     namespace, table_name = "pos_transactions", "transaction"
#
#     # --------------------------------------------------
#     # 1 Fetch data from MySQL source
#     # --------------------------------------------------
#
#     try:
#         mysql_start = time.time()
#         rows = mysql_creds.get_range(dbname, start_range, end_range)
#         mysql_duration = round(time.time() - mysql_start, 2)
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"MySQL fetch error: {str(e)}")
#
#     if not rows:
#         raise HTTPException(status_code=404, detail="No data found in the given range.")
#
#
#
#     # --------------------------------------------------
#     # Store to R2 and index metadata
#     # --------------------------------------------------
#
#
#     stored_count, failed_records = 0, []
#     for record in rows:
#
#         try:
#             customer_mobile = record["customer_mobile__c"]
#             pri_id = record["pri_id"]
#             bill_date_str = record.get("Bill_Date__c")
#             if bill_date_str:
#                 dt = safe_parse_date(bill_date_str)
#                 year = str(dt.year)
#                 month = str(dt.month).zfill(2)
#             else:
#                 year, month = "unknown", "unknown"
#
#             metadata = {
#                 "namespace": namespace,
#                 "table": table_name,
#                 "pri-id": str(pri_id),
#                 "customer-mobile": str(customer_mobile),
#                 "year": year,
#                 "month": month,
#                 "source-database": dbname,
#                 "fetched-at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
#                 "data-version": "v1.0",
#                 "status": "success",
#             }
#
#             r2_keys = [
#                 f"{namespace}/{table_name}/id/{pri_id}.json",
#                 f"{namespace}/{table_name}/phone/{year}/{month}/{customer_mobile}.json",
#                 f"{namespace}/{table_name}/phone_year_month/{customer_mobile}/{year}/{month}/{pri_id}.json"
#             ]
#
#
#             record_safe = make_json_serializable(record)
#             for key in r2_keys:
#                 store_json_to_r2(record_safe, key,metadata)
#
#             stored_count += 1
#
#
#         except Exception as e:
#             print(f"Error saving pri_id={record.get('pri_id')}: {e}")
#             failed_records.append(record.get('pri_id'))
#             continue
#
#     elapsed = round(time.time() - total_start, 2)
#
#     return {
#         "status": "success",
#         "namespace": namespace,
#         "table": table_name,
#         "rows_processed": len(rows),
#         "rows_stored": stored_count,
#         "failed_records": len(failed_records),
#         "mysql_fetch_seconds": mysql_duration,
#         "elapsed_seconds": elapsed
#     }


# @router.post("/create")
# async def transaction(
#     start_range: int = Query(0),
#     end_range: int = Query(100000)
# ):
#     if end_range <= start_range:
#         raise HTTPException(status_code=400, detail="end_range must be greater than start_range")
#
#     total_start = time.time()
#     mysql_creds = MysqlCatalog()
#     dbname = "Transaction"
#     namespace, table_name = "pos_transactions", "transaction"
#
#     # Step 1: Fetch data from MySQL
#     try:
#         mysql_start = time.time()
#         rows = mysql_creds.get_range(dbname, start_range, end_range)
#         mysql_duration = round(time.time() - mysql_start, 2)
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"MySQL fetch error: {e}")
#
#     if not rows:
#         raise HTTPException(status_code=404, detail="No data found in the given range.")
#
#     # Step 2: Group by (year, month, mobile)
#     grouped_data = defaultdict(list)
#     for record in rows:
#         try:
#             pri_id = record.get("pri_id")
#             customer_mobile = record.get("customer_mobile__c") or "unknown"
#             bill_date_str = record.get("Bill_Date__c")
#
#             if bill_date_str:
#                 dt = safe_parse_date(bill_date_str)
#                 year, month = str(dt.year), str(dt.month).zfill(2)
#             else:
#                 year, month = "unknown", "unknown"
#
#             record_safe = make_json_serializable(record)
#             grouped_data[(year, month, customer_mobile)].append(record_safe)
#
#         except Exception as e:
#             print(f"[Error parsing record] {e}")
#             continue
#
#     # Step 3: Merge with existing R2 JSON
#     for (year, month, customer_mobile), records in grouped_data.items():
#         key = f"{namespace}/{table_name}/phone/{year}/{month}/{customer_mobile}.json"
#
#         try:
#             # --- Try reading existing JSON ---
#             existing_data = []
#             try:
#                 obj = get_r2_client().get_object(Bucket=R2_BUCKET_NAME, Key=key)
#                 body = obj["Body"].read().decode("utf-8")
#                 loaded = json.loads(body)
#                 if isinstance(loaded, dict):
#                     existing_data = [loaded]
#                 elif isinstance(loaded, list):
#                     existing_data = loaded
#                 else:
#                     existing_data = []
#                     print(f"⚠️ Unexpected JSON type in {key}, resetting as empty list.")
#
#                 print(f"Found existing file: {key} with {len(existing_data)} records")
#
#                 # existing_data = json.loads(obj["Body"].read().decode("utf-8"))
#                 # print(f"Found existing file: {key} with {len(existing_data)} records")
#             except get_r2_client().exceptions.NoSuchKey:
#                 print(f"No existing file for {key}, creating new one.")
#             except Exception as e:
#                 print(f"Error reading {key}: {e}")
#
#             # --- Merge & deduplicate by pri_id ---
#             combined = existing_data + records
#             seen = {}
#             for r in combined:
#                 seen[r["pri_id"]] = r
#             merged = list(seen.values())
#
#             # --- Sort by pri_id ---
#             merged.sort(key=lambda x: str(x.get("pri_id", "")))
#
#             # --- Add metadata ---
#             metadata = {
#                 "namespace": namespace,
#                 "table": table_name,
#                 "year": year,
#                 "month": month,
#                 "customer-mobile": str(customer_mobile),
#                 "record-count": str(len(merged)),
#                 "source-database": dbname,
#                 "last-updated": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
#                 "status": "merged"
#             }
#
#             # --- Save merged JSON ---
#             # r2_client.put_object(
#             #     Bucket=R2_BUCKET,
#             #     Key=key,
#             #     Body=json.dumps(merged, indent=2).encode("utf-8"),
#             #     Metadata=metadata
#             # )
#             record_safe = make_json_serializable(merged)
#             store_json_to_r2(
#                 data=record_safe,
#                 meta_data=metadata,
#                 key=key
#             )
#
#             print(f"✅ Updated {key} with {len(merged)} total records.")
#
#         except Exception as e:
#             print(f"[R2 Write Error] {key}: {e}")
#             continue
#
#     elapsed = round(time.time() - total_start, 2)
#
#     return {
#         "status": "success",
#         "rows_processed": len(rows),
#         "unique_groups": len(grouped_data),
#         "mysql_fetch_seconds": mysql_duration,
#         "elapsed_seconds": elapsed
#     }

##############################################################################

#-------------------------------------------------------------------
#       direct database get with range
#-------------------------------------------------------------------
# @router.post("/create")
# async def transaction(
#     start_range: int = Query(0, description="Start row (e.g. 0)"),
#     end_range: int = Query(100000, description="End row (e.g. 100000)"),
# ):
#     if end_range <= start_range:
#         raise HTTPException(status_code=400, detail="end_range must be greater than start_range")
#
#     total_start = time.time()
#     mysql_creds = MysqlCatalog()
#
#     dbname = "Transaction"
#     namespace, table_name = "pos_transactions", "transaction"
#
#     try:
#         mysql_start = time.time()
#         rows = mysql_creds.get_range(dbname, start_range, end_range)
#         mysql_duration = round(time.time() - mysql_start, 2)
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"MySQL fetch error: {str(e)}")
#
#     if not rows:
#         raise HTTPException(status_code=404, detail="No data found in the given range.")
#
#     stored_count, failed_records = 0, []
#
#     for record in rows:
#         try:
#             record_safe = make_json_serializable(record)
#
#             customer_mobile = record.get("customer_mobile__c")
#             pri_id = record.get("pri_id")
#             bill_date_str = record.get("Bill_Date__c")
#
#             if bill_date_str:
#                 dt = safe_parse_date(bill_date_str)
#                 year, month = str(dt.year), str(dt.month).zfill(2)
#             else:
#                 year, month = "unknown", "unknown"
#
#             r2_keys = [
#                 f"{namespace}/{table_name}/id/{pri_id}.json",
#                 f"{namespace}/{table_name}/phone/{year}/{month}/{customer_mobile}.json",
#                 f"{namespace}/{table_name}/phone_year_month/{customer_mobile}/{year}/{month}/{pri_id}.json"
#                             ]
#             # key = f"{namespace}/{table_name}/phone/{year}/{month}/{customer_mobile}.json"
#             # existing = {}
#             for key in r2_keys:
#                 # store_json_to_r2(record_safe, key,metadata)
#
#             # --- Merge with existing file ---
#                 existing = load_r2_json(R2_BUCKET_NAME, key)
#                 existing.append(record_safe)
#
#             # Deduplicate and sort by pri_id
#                 merged = {r["pri_id"]: r for r in existing}.values()
#                 merged_sorted = sorted(merged, key=lambda x: str(x.get("pri_id", "")))
#
#             # --- Metadata ---
#                 metadata = {
#                     "namespace": namespace,
#                     "table_name": table_name,
#                     "year": year,
#                     "month": month,
#                     "customer_mobile": customer_mobile,
#                     "record_count": len(merged_sorted),
#                     "source": dbname,
#                     "last_updated": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
#                 }
#
#             # --- Save ---
#                 store_json_to_r2(merged_sorted, key, metadata)
#             stored_count += 1
#
#         except Exception as e:
#             print(f"Error saving pri_id={record.get('pri_id')}: {e}")
#             failed_records.append(record.get("pri_id"))
#             continue
#
#     elapsed = round(time.time() - total_start, 2)
#
#     return {
#         "status": "success",
#         "namespace": namespace,
#         "table": table_name,
#         "rows_processed": len(rows),
#         "rows_stored": stored_count,
#         "failed_records": len(failed_records),
#         "mysql_fetch_seconds": mysql_duration,
#         "elapsed_seconds": elapsed,
#     }

@router.get("/list")
def get_bucket_list(
    bucket_name: str = Query("dev-transaction", title="Bucket Name",description="Bucket name (default: dev-transaction)"),
    bucket_path: str = Query("pos_transactions", description="Folder path in R2 (default: pos_transactions)"),

):
    r2_client = get_r2_client()
    prefix = f"{bucket_path}/"

    try:
        response = r2_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        # print(response)

        files = []
        if "Contents" in response:
            files = [obj["Key"] for obj in response["Contents"]]

        return {
            "total_files": len(files),
            "files": files
        }

    except Exception as e:
        return {"error": str(e)}

# LOCAL_EXCEL_FOLDER = "excel"
# @router.post("/create")
# async def transaction(
#     start_range: int = Query(0, description="Start row (e.g. 0)"),
#     end_range: int = Query(100000, description="End row (e.g. 100000)"),
# ):
#     if end_range <= start_range:
#         raise HTTPException(status_code=400, detail="end_range must be greater than start_range")
#
#     os.makedirs(LOCAL_EXCEL_FOLDER, exist_ok=True)
#     total_start = time.time()
#     mysql_creds = MysqlCatalog()
#
#     namespace, table_name = "pos_transactions", "transaction"
#     dbname = "Transaction"
#
#     # 1 Fetch data
#     try:
#         mysql_start = time.time()
#         rows = mysql_creds.get_range(dbname, start_range, end_range)
#         mysql_duration = round(time.time() - mysql_start, 2)
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"MySQL fetch error: {str(e)}")
#
#     if not rows:
#         raise HTTPException(status_code=404, detail="No data found in the given range.")
#
#     # stored_count, failed_records = 0, []
#     # excel_records = []
#     stored_count, failed_records, error_logs, excel_records = 0, [], [], []
#
#     # 2️⃣ Process each record
#     for record in rows:
#         pri_id = record.get("pri_id", "unknown")
#         try:
#             record_safe = make_json_serializable(record)
#             customer_mobile = record.get("customer_mobile__c", "unknown")
#             # pri_id = record.get("pri_id")
#             bill_date_str = record.get("Bill_Date__c")
#
#             if bill_date_str:
#                 dt = safe_parse_date(bill_date_str)
#                 year, month = str(dt.year), str(dt.month).zfill(2)
#             else:
#                 year, month = "unknown", "unknown"
#
#             # 3️⃣ Multiple R2 keys
#             r2_keys = [
#                 f"{namespace}/{table_name}/id/{pri_id}.json",
#                 f"{namespace}/{table_name}/phone/{year}/{month}/{customer_mobile}.json",
#                 f"{namespace}/{table_name}/phone_year_month/{customer_mobile}/{year}/{month}/{pri_id}.json"
#             ]
#
#             for key in r2_keys:
#                 # --- Load existing data ---
#                 existing = load_r2_json(R2_BUCKET_NAME, key)
#                 existing.append(record_safe)
#
#                 # --- Deduplicate and sort by pri_id ---
#                 merged = {r["pri_id"]: r for r in existing}.values()
#                 merged_sorted = sorted(merged, key=lambda x: str(x.get("pri_id", "")))
#                 pri_ids = [r.get("pri_id") for r in merged_sorted]
#                 # --- Metadata ---
#                 metadata = {
#                     "pri_id": pri_ids,
#                     "Bill_Date": record.get("Bill_Date__c"),
#                     "customer_mobile": customer_mobile,
#                     "record_count": len(merged_sorted),
#                     "last_updated": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
#                 }
#                 # --- Save to R2 ---
#                 store_json_to_r2(merged_sorted, key, metadata)
#
#             stored_count += 1
#             excel_records.append(record_safe)
#
#         except Exception as e:
#             failed_records.append(pri_id)
#             error_logs.append({"pri_id": pri_id, "error": str(e)})
#             print(f"❌ Error saving pri_id={pri_id}: {e}")
#             continue
#
#     # excel_file_name = f"rows_{start_range}_{end_range}.xlsx"
#     excel_file_path = None
#     if excel_records:
#         try:
#             excel_file_name = f"rows_{start_range}_{end_range}.xlsx"
#             excel_file_path = os.path.join(LOCAL_EXCEL_FOLDER, excel_file_name)
#             pd.DataFrame(excel_records).to_excel(excel_file_path, index=False, engine="openpyxl")
#             print(f"✅ Excel saved locally: {excel_file_path}")
#         except Exception as e:
#             print(f"❌ Failed to save Excel: {e}")
#             excel_file_path = None
#
#     elapsed = round(time.time() - total_start, 2)
#
#     return {
#         "status": "success",
#         "namespace": namespace,
#         "table": table_name,
#         "rows_processed": len(rows),
#         "rows_stored": stored_count,
#         "failed_records": len(failed_records),
#         "mysql_fetch_seconds": mysql_duration,
#         "elapsed_seconds": elapsed,
#     }

# multi threading
import concurrent.futures

# LOCAL_EXCEL_FOLDER = "excel"
# MAX_WORKERS = 30

@router.post("/create")
async def transaction(
    start_range: int = Query(0, description="Start row (e.g. 0)"),
    end_range: int = Query(100000, description="End row (e.g. 100000)"),
):
    if end_range <= start_range:
        raise HTTPException(status_code=400, detail="end_range must be greater than start_range")

    # os.makedirs(LOCAL_EXCEL_FOLDER, exist_ok=True)
    total_start = time.time()
    mysql_creds = MysqlCatalog()

    namespace, table_name = "pos_transactions", "transaction"
    dbname = "Transaction"

    # --- 1️⃣ Fetch MySQL data ---
    try:
        mysql_start = time.time()
        rows = mysql_creds.get_range(dbname, start_range, end_range)
        mysql_duration = round(time.time() - mysql_start, 2)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"MySQL fetch error: {str(e)}")

    if not rows:
        raise HTTPException(status_code=404, detail="No data found in the given range.")

    stored_count, failed_records, error_logs, excel_records = 0, [], [], []

    # --- 2️⃣ Function to process each record ---
    def process_record(record: dict[str, Any]):
        pri_id = record.get("pri_id", "unknown")
        try:
            record_safe = make_json_serializable(record)
            customer_mobile = record.get("customer_mobile__c", "unknown")
            bill_date_str = record.get("Bill_Date__c")

            if bill_date_str:
                dt = safe_parse_date(bill_date_str)
                year, month = str(dt.year), str(dt.month).zfill(2)
            else:
                year, month = "unknown", "unknown"

            r2_keys = [
                f"{namespace}/{table_name}/id/{pri_id}.json",
                f"{namespace}/{table_name}/phone/{year}/{month}/{customer_mobile}.json",
                f"{namespace}/{table_name}/phone_year_month/{customer_mobile}/{year}/{month}/{pri_id}.json",
            ]

            for key in r2_keys:
                existing = load_r2_json(R2_BUCKET_NAME, key)
                existing.append(record_safe)

                merged = {r["pri_id"]: r for r in existing}.values()
                merged_sorted = sorted(merged, key=lambda x: str(x.get("pri_id", "")))
                pri_ids = [r.get("pri_id") for r in merged_sorted]

                metadata = {
                    "pri_id": pri_ids,
                    "Bill_Date": record.get("Bill_Date__c"),
                    "customer_mobile": customer_mobile,
                    "record_count": len(merged_sorted),
                    "last_updated": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
                }

                store_json_to_r2(merged_sorted, key, metadata)

            return {"status": "ok", "pri_id": pri_id, "record": record_safe}

        except Exception as e:
            return {"status": "error", "pri_id": pri_id, "error": str(e)}

    # --- 3️⃣ Run in parallel threads ---
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_record, r) for r in rows]

        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result["status"] == "ok":
                stored_count += 1
                excel_records.append(result["record"])
            else:
                failed_records.append(result["pri_id"])
                error_logs.append(result)

    # --- 4️⃣ Save Excel output ---
    # excel_file_path = None
    # if excel_records:
    #     try:
    #         excel_file_name = f"rows_{start_range}_{end_range}.xlsx"
    #         excel_file_path = os.path.join(LOCAL_EXCEL_FOLDER, excel_file_name)
    #         pd.DataFrame(excel_records).to_excel(excel_file_path, index=False, engine="openpyxl")
    #         print(f"✅ Excel saved locally: {excel_file_path}")
    #     except Exception as e:
    #         print(f"❌ Failed to save Excel: {e}")
    #         excel_file_path = None

    elapsed = round(time.time() - total_start, 2)

    # --- 5️⃣ Return summary ---
    return {
        "status": "success",
        "namespace": namespace,
        "table": table_name,
        "rows_processed": len(rows),
        "rows_stored": stored_count,
        "failed_records": len(failed_records),
        "mysql_fetch_seconds": mysql_duration,
        "elapsed_seconds": elapsed,
        # "excel_file": excel_file_path,
    }

# @router.post("/create-pri-id")
# async def transaction(
#     start_range: int = Query(0, description="Start row (e.g. 0)"),
#     end_range: int = Query(100000, description="End row (e.g. 100000)"),
# ):
#     if end_range <= start_range:
#         raise HTTPException(status_code=400, detail="end_range must be greater than start_range")
#
#     total_start = time.time()
#     mysql_creds = MysqlCatalog()
#
#     namespace, table_name = "pos_transactions", "transaction"
#     dbname = "Transaction"
#
#     try:
#         mysql_start = time.time()
#         rows = mysql_creds.get_range(dbname, start_range, end_range)
#         mysql_duration = round(time.time() - mysql_start, 2)
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"MySQL fetch error: {str(e)}")
#
#     if not rows:
#         raise HTTPException(status_code=404, detail="No data found in the given range.")
#
#     stored_count, failed_records, error_logs, excel_records = 0, [], [], []
#
#
#     def process_record(record: dict[str, Any]):
#         pri_id = record.get("pri_id", "unknown")
#         try:
#             record_safe = make_json_serializable(record)
#
#             # Only 1 path: id-based JSON
#             key = f"id/{pri_id}.json"
#
#             # Load existing data (if file exists in R2)
#             existing = load_r2_json(R2_BUCKET_NAME, key)
#             existing.append(record_safe)
#
#
#             # Metadata for reference
#             metadata = {
#                 "pri_ids": pri_id,
#                 "last_updated": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
#             }
#
#             # Store only one JSON file per pri_id
#             store_json_to_r2(existing, key, metadata)
#
#             return {"status": "ok", "pri_id": pri_id, "record": record_safe}
#
#         except Exception as e:
#             return {"status": "error", "pri_id": pri_id, "error": str(e)}
#
#     # --- Run parallel threads ---
#     with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
#         futures = [executor.submit(process_record, r) for r in rows]
#
#         for future in concurrent.futures.as_completed(futures):
#             result = future.result()
#             if result["status"] == "ok":
#                 stored_count += 1
#                 excel_records.append(result["record"])
#             else:
#                 failed_records.append(result["pri_id"])
#                 error_logs.append(result)
#
#     elapsed = round(time.time() - total_start, 2)
#
#     # ---  Return summary ---
#     return {
#         "status": "success",
#         "rows_processed": len(rows),
#         "rows_stored": stored_count,
#         "failed_records": len(failed_records),
#         "elapsed_seconds": elapsed,
#         "r2_key_pattern": f"id/<pri_id>.json",
#     }

##############################################################################

#---------------------------------------------------
# direct body raw query language
#---------------------------------------------------

@router.post("/create_single")
async def transaction(
        record: dict = Body(..., description="Single JSON record to insert into Iceberg"),
):
    total_start = time.time()
    namespace, table_name = "pos_transactions", "transaction"

    if not record:
        raise HTTPException(status_code=404, detail="No data found.")

    stored_count, failed_records = 0, []

    try:
        record_safe = make_json_serializable(record)

        customer_mobile = record.get("customer_mobile__c") or "unknown"
        pri_id = record.get("pri_id")
        bill_date_str = record.get("Bill_Date__c")

        if bill_date_str:
            dt = safe_parse_date(bill_date_str)
            year, month = str(dt.year), str(dt.month).zfill(2)
        else:
            year, month = "unknown", "unknown"

        r2_keys = [
            f"{namespace}/{table_name}/id/{pri_id}.json",
            f"{namespace}/{table_name}/phone/{year}/{month}/{customer_mobile}.json",
            f"{namespace}/{table_name}/phone_year_month/{customer_mobile}/{year}/{month}/{pri_id}.json"
        ]
        # key = f"{namespace}/{table_name}/phone/{year}/{month}/{customer_mobile}.json"

        for key in r2_keys:
            # --- Merge with existing file ---
            existing = load_r2_json(R2_BUCKET_NAME, key)
            existing.append(record_safe)

            # Deduplicate and sort by pri_id
            merged = {r["pri_id"]: r for r in existing}.values()
            merged_sorted = sorted(merged, key=lambda x: str(x.get("pri_id", "")))
            pri_ids = [r.get("pri_id") for r in merged_sorted]
            # --- Metadata ---

            metadata = {
                "pri_id": pri_ids,
                "Bill_Date": record.get("Bill_Date__c"),
                "customer_mobile": customer_mobile,
                "record_count": len(merged_sorted),
                "last_updated": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
            }

            # --- Save ---
            store_json_to_r2(merged_sorted, key, metadata)
        stored_count += 1

    except Exception as e:
        print(f"❌ Error saving pri_id={record.get('pri_id')}: {e}")
        failed_records.append(record.get("pri_id"))

    elapsed = round(time.time() - total_start, 2)

    return {
        "status": "success",
        "namespace": namespace,
        "table": table_name,
        "rows_stored": stored_count,
        "failed_records": len(failed_records),
        "elapsed_seconds": elapsed,
    }



def format_response(data: Any, metadata: dict):
    if isinstance(data, list):
        count = len(data)
    elif isinstance(data, dict):
        count = 1
        data = [data]  # make uniform
    else:
        count = 0
        data = []
    return {"count": count, "metadata": metadata, "data": data}

##############################################################################

#------------------------------------------------------------
# pri_id
#------------------------------------------------------------

# 1 Get by Primary ID
@router.get("/get-by-id")
def get_by_id(
        pri_id: int
    ):
    namespace, table_name = "pos_transactions", "transaction"

    key = f"{namespace}/{table_name}/id/{pri_id}.json"

    try:
        data = fetch_json_from_r2(key)
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Record not found: {pri_id}")
    return {
        "status": "success",
        "namespace": namespace,
        "table": table_name,
        "data": data
    }

#------------------------------------------------------------
# Get by Customer Phone (Year + Month)
#------------------------------------------------------------

@router.get("/get-by-phone")
def get_by_phone(

    year: str = Query(..., description="Year, e.g., '2025'"),
    month: str = Query(..., description="Month, e.g., '10'"),
    phone: str = Query(..., description="Customer phone number")
    ):

    namespace, table_name = "pos_transactions", "transaction"

    key = f"{namespace}/{table_name}/phone/{year}/{month}/{phone}.json"
    try:
        data = fetch_json_from_r2(key)
        print("count:",len(data))
    except Exception:
        raise HTTPException(status_code=404, detail=f"No records for {phone} in {year}-{month}")

    return {
        "status": "success",
        "namespace": namespace,
        "table": table_name,
        "count": len(data),
        "data": data
    }

# @router.get("/get-by-phone-year-month")
# def get_by_phone_year_month(customer_mobile: str):
#     namespace, table_name = "pos_transactions", "transaction"
#     prefix = f"{namespace}/{table_name}/phone_year_month/{customer_mobile}/"
#     print(f"🔍 Searching prefix: {prefix}")
#
#     try:
#         object_keys = list_r2_objects(prefix)
#         response = s3.list_objects_v2(Bucket=R2_BUCKET_NAME, Prefix=prefix)
#         print(response)
#         # object_keys = fetch_json_from_r2(prefix)
#         print("data",object_keys)
#         print(f"✅ Found {len(object_keys)} objects:", object_keys)
#         if "Contents" not in response:
#             raise HTTPException(status_code=404, detail=f"No records found for {customer_mobile}")
#
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Error listing R2 objects: {e}")
#
#     if not object_keys:
#         raise HTTPException(status_code=404, detail=f"No records found for {customer_mobile}")
#
#     results = []
#     for key in object_keys:
#         try:
#             data = fetch_json_from_r2(key)
#             parts = key.split("/")
#             year, month, pri_id_json = parts[-3], parts[-2], parts[-1]
#             pri_id = pri_id_json.replace(".json", "")
#             data["_metadata"] = {"year": year, "month": month, "pri_id": pri_id}
#             results.append(data)
#         except Exception as e:
#             print(f" Error reading {key}: {e}")
#             continue
#
#     return format_response(results)



# 3️⃣ Get by Customer + Year + Month + PRI ID
# def get_by_phone_year_month(customer_mobile: str):
#     namespace, table_name = "pos_transactions", "transaction"
#     print("phone",customer_mobile)
#     prefix = f"{namespace}/{table_name}/phone_year_month/{customer_mobile}/"
#     try:
#         object_keys = fetch_json_from_r2(prefix)
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Error listing R2 objects: {e}")
#
#     if not object_keys:
#         raise HTTPException(status_code=404, detail=f"No records found for {customer_mobile}")
#
#     results = []
#     for key in object_keys:
#         try:
#             print(key)
#             data = fetch_json_from_r2(key)
#             # extract metadata from path
#             parts = key.split("/")
#             year, month, pri_id_json = parts[-3], parts[-2], parts[-1]
#             pri_id = pri_id_json.replace(".json", "")
#             data["_metadata"] = {"year": year, "month": month, "pri_id": pri_id}
#             results.append(data)
#         except Exception as e:
#             print(f" Error reading {key}: {e}")
#             continue
#
#     return format_response(results)



@router.get("/get-by-phone-year-month")
def get_by_phone_year_month(customer_mobile: str):
    namespace, table_name = "pos_transactions", "transaction"
    prefix = f"{namespace}/{table_name}/phone_year_month/{customer_mobile}/"

    try:
        # Correct API call: Prefix, not Key
        response = s3.list_objects_v2(Bucket=R2_BUCKET_NAME, Prefix=prefix)

        if "Contents" not in response:
            raise HTTPException(status_code=404, detail=f"No records found for {customer_mobile}")

        results = []
        for obj in response["Contents"]:
            key = obj["Key"]
            content = load_r2_json(R2_BUCKET_NAME, key)

            # Extract metadata
            # pri_id = content.get("pri_id", "unknown")
            # year = content.get("year", "unknown")
            # month = content.get("month", "unknown")
            for record in content:
                results.append({
                    "r2_path": key,
                    "pri_id": record.get("pri_id", "unknown"),
                    "year": record.get("year", "unknown"),
                    "month": record.get("month", "unknown"),
                    "data": record
            })

        return {
            "customer_mobile": customer_mobile,
            "count": len(results),
            "records": results
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing R2 objects: {e}")


@router.delete("/delete-fileses")
def delete_files(
        bucket_name: str = Query(..., title="Bucket Name",description="Bucket name (default:dev-transaction)"),
        bucket_path: str = Query("pos_transactions", description="Folder path in R2 (default: pos_transactions)"),
        # bucket_name: str = Query(..., title="Bucket Name"),
        # bucket_path: str = Query("iceberg_json", description="Folder path in R2 (default: iceberg_json)"),
        prefix_only: bool = Query(True, description="Delete all files under prefix (True) or specific file (False)"),
        file_name: str = Query(None, description="Specific file name (e.g. 'batch_0.json') if prefix_only=False")
):

    r2_client = get_r2_client()
    prefix = f"{bucket_path}/"
    print("Deleting files")
    print(f"{prefix}")
    try:
        deleted_files = []

        if prefix_only:
            response = r2_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
            if "Contents" in response:
                for obj in response["Contents"]:
                    print(obj["Key"])
                    r2_client.delete_object(Bucket=bucket_name, Key=obj["Key"])
                    deleted_files.append(obj["Key"])
        else:
            if not file_name:
                return {"error": "file_name is required if prefix_only=False"}

            file_key = prefix + file_name
            r2_client.delete_object(Bucket=bucket_name, Key=file_key)
            deleted_files.append(file_key)

        return {
            "message": f"{len(deleted_files)} file(s) deleted",
            "deleted_files": deleted_files
        }

    except Exception as e:
        return {"error": str(e)}


# @router.post("/create-pri-id")
# async def create_pri_id_records(
#     start_range: int = Query(0, description="Start row (e.g., 0)"),
#     end_range: int = Query(100000, description="End row (e.g., 100000)")
# ):
#
#     if end_range <= start_range:
#         raise HTTPException(status_code=400, detail="end_range must be greater than start_range")
#
#     total_start = time.time()
#     mysql_creds = MysqlCatalog()
#
#     namespace, table_name = "pos_transactions", "transaction"
#     dbname = "Transaction"
#
#     # --- Step 1: Fetch records from MySQL ---
#     try:
#         mysql_start = time.time()
#         rows = mysql_creds.get_range(dbname, start_range, end_range)
#         mysql_duration = round(time.time() - mysql_start, 2)
#         if not rows:
#             raise HTTPException(status_code=404, detail="No data found in the given range.")
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"MySQL fetch error: {str(e)}")
#
#     stored_count = 0
#     failed_records = []
#     error_logs = []
#     excel_records = []
#
#     # --- Step 2: Define record processor ---
#     def process_record(record: dict[str, Any]):
#         pri_id = record.get("pri_id")
#         if not pri_id:
#             return {"status": "error", "error": "Missing pri_id"}
#
#         try:
#             record_safe = make_json_serializable(record)
#             key = f"id/{pri_id}.json"
#
#             # Load existing JSON data (if any)
#             existing = load_r2_json(R2_BUCKET_NAME, key)
#             existing.append(record_safe)
#
#             # Metadata tracking
#             metadata = {
#                 "pri_id": pri_id,
#                 "last_updated": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
#             }
#
#             # Store JSON file to R2 (overwriting with updated list)
#             store_json_to_r2(existing, key, metadata)
#
#             return {"status": "ok", "pri_id": pri_id}
#
#         except Exception as e:
#             return {"status": "error", "pri_id": pri_id, "error": str(e)}
#
#     # --- Step 3: Parallel processing ---
#     with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
#         for result in executor.map(process_record, rows):
#             if result["status"] == "ok":
#                 stored_count += 1
#                 excel_records.append(result["pri_id"])
#             else:
#                 failed_records.append(result.get("pri_id"))
#                 error_logs.append(result)
#
#     total_elapsed = round(time.time() - total_start, 2)
#
#     # --- Step 4: Return summary ---
#     return {
#         "status": "success",
#         "mysql_duration_sec": mysql_duration,
#         "rows_fetched": len(rows),
#         "rows_stored": stored_count,
#         "failed_count": len(failed_records),
#         "elapsed_total_sec": total_elapsed,
#         "r2_key_pattern": "id/<pri_id>.json",
#         "errors": error_logs[:5],  # Limit output for readability
#     }
from fastapi import APIRouter, HTTPException, Query
from datetime import datetime
from typing import Any, List
import time
import concurrent.futures

# @router.post("/create-pri-id")
# async def create_pri_id_records(
#     start_range: int = Query(0, description="Start row (e.g., 0)"),
#     end_range: int = Query(100000, description="End row (e.g., 100000)")
# ):
#     """
#     Fetch records from MySQL and upload each record to R2 storage as a JSON file.
#     Each file is named as 'id/<pri_id>.json'.
#     """
#     if end_range <= start_range:
#         raise HTTPException(status_code=400, detail="end_range must be greater than start_range")
#
#     total_start = time.time()
#     mysql_creds = MysqlCatalog()
#     namespace, table_name = "pos_transactions", "transaction"
#     dbname = "Transaction"
#
#     # --- Step 1: Fetch records from MySQL ---
#     try:
#         mysql_start = time.time()
#         rows = mysql_creds.get_range(dbname, start_range, end_range)
#         mysql_duration = round(time.time() - mysql_start, 2)
#         if not rows:
#             raise HTTPException(status_code=404, detail="No data found in the given range.")
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"MySQL fetch error: {e}")
#
#     stored_count = 0
#     failed_records: List[Any] = []
#     error_logs: List[Any] = []
#     excel_records: List[Any] = []
#
#     # --- Step 2: Optimized record processor ---
#     def process_record(record: dict[str, Any]):
#         pri_id = record.get("pri_id")
#         if not pri_id:
#             return {"status": "error", "error": "Missing pri_id"}
#
#         try:
#             # Prepare JSON-safe record
#             record_safe = make_json_serializable(record)
#             key = f"id/{pri_id}.json"
#
#             # Fetch existing data (if any)
#             existing = load_r2_json(R2_BUCKET_NAME, key)
#             existing.append(record_safe)
#
#             # Add metadata
#             metadata = {
#                 "pri_id": pri_id,
#                 "last_updated": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
#             }
#
#             # Save updated JSON to R2
#             store_json_to_r2(existing, key, metadata)
#             return {"status": "ok", "pri_id": pri_id}
#
#         except Exception as e:
#             return {"status": "error", "pri_id": pri_id, "error": str(e)}
#
#     # --- Step 3: Parallel processing with batch partitioning ---
#     # Reduces overhead by processing in manageable chunks
#     def process_in_batches(data, batch_size=1000):
#         for i in range(0, len(data), batch_size):
#             yield data[i:i + batch_size]
#
#     for batch in process_in_batches(rows):
#         with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
#             results = list(executor.map(process_record, batch))
#
#         for result in results:
#             if result["status"] == "ok":
#                 stored_count += 1
#                 excel_records.append(result["pri_id"])
#             else:
#                 failed_records.append(result.get("pri_id"))
#                 error_logs.append(result)
#
#     total_elapsed = round(time.time() - total_start, 2)
#
#     # --- Step 4: Return summary ---
#     return {
#         "status": "success",
#         "mysql_duration_sec": mysql_duration,
#         "rows_fetched": len(rows),
#         "rows_stored": stored_count,
#         "failed_count": len(failed_records),
#         "elapsed_total_sec": total_elapsed,
#         "r2_key_pattern": "id/<pri_id>.json",
#         "errors": error_logs[:5],
#     }

MAX_WORKERS = 16
import concurrent.futures
import math
BATCH_SIZE = 1000

@router.post("/create-pri-id")
async def create_pri_id_records(
    start_range: int = Query(0, description="Start row (e.g., 0)"),
    end_range: int = Query(100000, description="End row (e.g., 100000)")
):
    """
    Fetch records from MySQL and upload each as JSON to R2.
    Includes batch-based parallelism and detailed time logs.
    """

    if end_range <= start_range:
        raise HTTPException(status_code=400, detail="end_range must be greater than start_range")

    total_start = time.time()
    mysql_creds = MysqlCatalog()
    dbname = "Transaction"

    # --- Step 1: Fetch from MySQL ---
    try:
        mysql_start = time.time()
        rows = mysql_creds.get_range(dbname, start_range, end_range)
        mysql_duration = round(time.time() - mysql_start, 2)
        if not rows:
            raise HTTPException(status_code=404, detail="No data found in the given range.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"MySQL fetch error: {e}")

    stored_count, failed_count = 0, 0
    error_logs: List[Any] = []

    # --- Helper: Single record handler ---
    def process_record(record: dict[str, Any]):
        pri_id = record.get("pri_id")
        if not pri_id:
            return {"status": "error", "error": "Missing pri_id"}

        try:
            record_safe = make_json_serializable(record)
            key = f"id/{pri_id}.json"

            # Load existing once; append and write back
            # existing = load_r2_json(R2_BUCKET_NAME, key)
            # existing.append(record_safe)

            metadata = {
                "pri_id": pri_id,
                "last_updated": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
            }
            store_json_to_r2([record_safe], key, metadata)
            return {"status": "ok"}

        except Exception as e:
            return {"status": "error", "error": str(e)}

    # --- Step 2: Batch Processing ---
    total_rows = len(rows)
    num_batches = math.ceil(total_rows / BATCH_SIZE)

    for batch_index in range(num_batches):
        batch_start_idx = batch_index * BATCH_SIZE
        batch_end_idx = min(batch_start_idx + BATCH_SIZE, total_rows)
        batch_data = rows[batch_start_idx:batch_end_idx]

        print(f"🟡 Processing Batch {batch_index + 1}/{num_batches} → Rows {batch_start_idx}–{batch_end_idx}")

        batch_start_time = time.time()

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            results = list(executor.map(process_record, batch_data))

        batch_duration = round(time.time() - batch_start_time, 2)

        # Collect stats
        ok = sum(1 for r in results if r["status"] == "ok")
        err = len(results) - ok
        stored_count += ok
        failed_count += err
        error_logs.extend(r for r in results if r["status"] == "error")

        print(f"✅ Batch {batch_index + 1} Completed in {batch_duration}s ({ok} ok / {err} failed)")

    total_elapsed = round(time.time() - total_start, 2)

    # --- Step 3: Summary ---
    return {
        "status": "success",
        "mysql_duration_sec": mysql_duration,
        "rows_fetched": total_rows,
        "rows_stored": stored_count,
        "failed_count": failed_count,
        "elapsed_total_sec": total_elapsed,
        "batches": num_batches,
        "r2_key_pattern": "id/<pri_id>.json",
        "errors": error_logs[:5],
    }

@router.get("/multipart_upload")
def list_multipart_uploads(bucket_name: str):
    resp = s3.list_multipart_uploads(Bucket=bucket_name)
    uploads = resp.get('Uploads', [])
    print("FOUND:", len(uploads))

    for u in uploads:
        key = u['Key']
        upload_id = u['UploadId']
        print("ABORT:", key, upload_id)
        s3.abort_multipart_upload(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_id
        )

    print("DONE")