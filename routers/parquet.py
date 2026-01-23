from core.r2_client import get_r2_client
from core.catalog_client import get_catalog_client
from core.r2_client import get_r2_client
from fastapi import APIRouter, HTTPException, Query
import time
import os
import pyarrow.parquet as pq
import pyarrow.fs as fs


router = APIRouter(prefix="", tags=["parquet"])

def get_s3_fs():
    return fs.S3FileSystem(
        access_key=os.getenv("ACCESS_KEY_ID"),
        secret_key=os.getenv("SECRET_ACCESS_KEY"),
        endpoint_override=os.getenv("ENDPOINT"),
    )

@router.get("/parquet/list")
def list_parquet(
        namespace: str = Query("POS_transactions"),
        table_name: str = Query("Transaction"),
        limit: int = 100
):
    """
    List parquet files for given Iceberg table (from manifest)
    """
    start = time.perf_counter()
    catalog = get_catalog_client()

    try:
        tbl = catalog.load_table(f"{namespace}.{table_name}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"load table failed: {str(e)}")

    files = []
    snap = tbl.current_snapshot()
    
    if not snap:
        return {
            "count": 0,
            "files": [],
            "time_seconds": round(time.perf_counter() - start, 3),
        }

    for manifest in snap.manifests(tbl.io):
        if len(files) >= limit:
            break
            
        entries = manifest.fetch_manifest_entry(tbl.io)
        
        for enter in entries:
            if len(files) >= limit:
                break
                
            df = enter.data_file
            files.append({
                "path": df.file_path,
                "rows": df.record_count,
                "size_bytes": df.file_size_in_bytes,
            })

    return {
        "count": len(files),
        "files": files,
        "time_seconds": round(time.perf_counter() - start, 3) 
    }


# @router.get("/parquet/read")
# def read_parquet(
#     path: str = Query(..., description="Full s3:// R2 parquet path"),
#     limit: int = Query(10)
# ):
#     try:
#         s3fs = get_s3_fs()
#         r2_client = get_r2_client()
#
#         # ✅ Normalize path for PyArrow: remove s3:// prefix
#         if path.startswith("s3://"):
#             path = path.replace("s3://", "", 1)
#
#         # Memory safe read using ParquetFile
#         pq_file = pq.ParquetFile(path, filesystem=s3fs)
#
#         # iter_batches allows reading chunks without loading whole file
#         # batch_size is target, actual size might vary slightly but safest for memory
#         try:
#             batch_iter = pq_file.iter_batches(batch_size=limit)
#             table = next(batch_iter)
#             df = table.to_pandas()
#
#             # Slice to exact limit if batch was larger
#             if len(df) > limit:
#                 df = df.head(limit)
#
#             return {
#                 "status": "success",
#                 "path": path,
#                 "row_count_file": pq_file.metadata.num_rows,
#                 "sample_rows": df.to_dict(orient="records")
#             }
#         except StopIteration:
#             # Handle empty files
#              return {
#                 "status": "success",
#                 "path": path,
#                 "row_count_file": 0,
#                 "sample_rows": []
#             }
#
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Failed to read parquet: {str(e)}")

import numpy as np
@router.get("/parquet/read")
def read_parquet(
    path: str = Query(..., description="Full s3:// R2 parquet path"),
    limit: int = Query(100, gt=0, le=10_000)
):
    try:
        s3fs = get_s3_fs()

        # Normalize s3:// path
        normalized_path = path.replace("s3://", "", 1)

        pq_file = pq.ParquetFile(normalized_path, filesystem=s3fs)

        if pq_file.metadata.num_rows == 0:
            return {
                "status": "success",
                "path": path,
                "row_count_file": 0,
                "sample_rows": []
            }

        batch = next(pq_file.iter_batches(batch_size=limit), None)

        if batch is None:
            return {
                "status": "success",
                "path": path,
                "row_count_file": pq_file.metadata.num_rows,
                "sample_rows": []
            }

        df = batch.to_pandas().head(limit)

        # 🔥 CRITICAL FIX: Make JSON safe
        df = df.replace([np.nan, np.inf, -np.inf], None)

        return {
            "status": "success",
            "path": path,
            "row_count_file": pq_file.metadata.num_rows,
            "returned_rows": len(df),
            "sample_rows": df.to_dict(orient="records")
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to read parquet file: {str(e)}"
        )

# @router.get("/parquet/read-to-json")
# def read_parquet_conver_to_json(
#         path: str = Query(..., description="Full s3:// R2 parquet path"),
#         limit: int = Query(10)
# ):
#     try:
#         s3fs = get_s3_fs()
#
#         if path.startswith("s3://"):
#             path = path.replace("s3://", "", 1)
#
#         # Memory safe read using ParquetFile
#         pq_file = pq.ParquetFile(path, filesystem=s3fs)
#
#         # Read the first batch to get sample data and schema
#         # If limit is small, this might be enough.
#         # For this endpoint, we will just read up to 'limit' rows total.
#
#         batch_iter = pq_file.iter_batches(batch_size=limit)
#         try:
#             table = next(batch_iter)
#             df = table.to_pandas()
#
#             record_count = 0
#             print("df",df)
#
#             for batch in pq_file.iter_batches():
#                 df = batch.to_pandas()
#                 # Convert to list of dicts
#                 records = df.to_dict(orient="records")
#                 for record in records:
#                     f.write(json.dumps(record) + "\n")
#                 record_count += len(records)
#             try:
#                 r2 = get_r2_client()
#                 bucket = "dev-transaction"
#                 invoice_id_key = invoice_id.replace("/", "_")
#                 invoice_key = f"mobile/{mobile}/invoice/{invoice_id_key}.json"
#
#                 file_obj = r2.get_object(Bucket=bucket, Key=invoice_key)
#
#                 raw = file_obj["Body"].read().decode("utf-8")
#                 raw = raw.replace("NaN", "null").replace("Infinity", "null").replace("-Infinity", "null")
#                 data = json.loads(raw)
#
#
#             return {
#                 "status": "success",
#                 "path": path,
#                 "row_count_file": pq_file.metadata.num_rows,
#                 "sample_rows": df.to_dict(orient="records")
#             }
#         except StopIteration:
#             # Handle empty files
#             return {
#                 "status": "success",
#                 "path": path,
#                 "row_count_file": 0,
#                 "sample_rows": []
#             }
#
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Failed to read parquet: {str(e)}")

    # @router.post("/parquet/merge-parquet")
# def merge_parquet(
#     namespace: str = Query("pos_transactions"),
#     table_name: str = Query("iceberg_with_partitioning"),
#     max_rows: int = Query(20000, description="merge files smaller than this row count"),
# ):
#     start = time.perf_counter()
#     catalog = get_catalog_client()
#     tbl = catalog.load_table(f"{namespace}.{table_name}")

#     snap = tbl.current_snapshot()
#     if not snap:
#         raise HTTPException(status_code=400, detail="no snapshot")

#     # 1) find small files
#     small_files = []
#     for manifest in snap.manifests(tbl.io):
#         entries = manifest.fetch_manifest_entry(tbl.io)
#         for e in entries:
#             df = e.data_file
#             if df.record_count < max_rows:
#                 small_files.append(df.file_path)

#     if not small_files:
#         return {"message": "no small files to merge", "small_files_count": len(small_files)}

#     print("small files count:", len(small_files))
    # 2) read small files
    # s3fs = get_s3_fs()

    # tables = []
    # for p in small_files:
    #     raw = p.replace("s3://", "", 1) if p.startswith("s3://") else p
    #     t = pq.read_table(raw, filesystem=s3fs)
    #     tables.append(t)

    # merged = pa.concat_tables(tables)

    # # 3) append new merged file
    # tbl.append(merged)

    # # 4) delete old small files (AFTER append)
    # # Using s3fs to delete files instead of catalog client
    # deleted_count = 0
    # for p in small_files:
    #     raw = p.replace("s3://", "", 1) if p.startswith("s3://") else p
    #     try:
    #         # Ensure we are deleting the exact object
    # s3fs = get_s3_fs()
    #
    # tables = []
    # for p in small_files:
    #     raw = p.replace("s3://", "", 1) if p.startswith("s3://") else p
    #     t = pq.read_table(raw, filesystem=s3fs)
    #     tables.append(t)
    #
    # merged = pa.concat_tables(tables)
    #
    # # 3) append new merged file
    # tbl.append(merged)
    #
    # # 4) delete old small files (AFTER append)
    # # Using s3fs to delete files instead of catalog client
    # deleted_count = 0
    # for p in small_files:
    #     raw = p.replace("s3://", "", 1) if p.startswith("s3://") else p
    #     try:
    #         # Ensure we are deleting the exact object
    #         s3fs.delete_file(raw)
    #         print("deleted:", raw)
    #         deleted_count += 1
    #     except Exception as e:
    #         print("delete failed:", raw, e)
    #
    # return {
    #     "merged_files_count": len(small_files),
    #     "deleted_old_files": deleted_count,
    #     "new_file_rows": merged.num_rows,
    #     "seconds": round(time.perf_counter() - start, 3),
    #     "status": "merged + old files deleted"
    # }


# class ParquetService:
#
#     @staticmethod
#     def parquet_to_json(parquet_path: str, output_path: str):
#         """
#         Read a Parquet file and convert to JSON file.
#         """
#
#         parquet_file = pq.ParquetFile(parquet_path)
#         record_count = 0
#
#         with open(output_path, "w") as f:
#             for batch in parquet_file.iter_batches():
#                 df = batch.to_pandas()
#                 # Convert to list of dicts
#                 records = df.to_dict(orient="records")
#                 for record in records:
#                     f.write(json.dumps(record) + "\n")
#                 record_count += len(records)
#
#         return output_path, record_count
#
# # services/r2_service.py
# import boto3
# from botocore.client import Config
#
#
# class R2Service:
#
#     def __init__(self, account_id, access_key, secret_key, bucket):
#         self.bucket = bucket
#         self.client = boto3.client(
#             "s3",
#             endpoint_url=f"https://{account_id}.r2.cloudflarestorage.com",
#             aws_access_key_id=access_key,
#             aws_secret_access_key=secret_key,
#             config=Config(signature_version="s3v4")
#         )
#
#     def upload(self, file_path: str, bucket_path: str) -> str:
#         file_name = file_path.split("/")[-1]
#         key = f"{bucket_path}/{file_name}"
#
#         self.client.upload_file(file_path, self.bucket, key)
#
#         return f"r2://{self.bucket}/{key}"
#
# @router.post("/parquet-to-json")
# def convert_parquet_to_json(
#         parquet_path: str,
#         bucket_path: str = "converted/json"
# ):
#     """
#     1. Read Parquet
#     2. Convert to JSON
#     3. Upload to R2 bucket
#     """
#
#     if not Path(parquet_path).exists():
#         raise HTTPException(status_code=404, detail="Parquet file not found")
#
#     # Use a safe temporary file logic
#     fd, json_path = tempfile.mkstemp(suffix=".json")
#     os.close(fd) # Close the low-level handle, we will open it via Python
#
#     try:
#         # Step 1: Convert
#         _, record_count = ParquetService.parquet_to_json_lines(parquet_path, json_path)
#
#         # Step 2: Upload (assuming r2_service is available)
#         # bucket_url = r2_service.upload(json_path, bucket_path)
#         bucket_url = "http://mock-upload-url" # Placeholder
#         return {
#             "status": "success",
#             "records": record_count,
#             "r2_url": bucket_url
#         }
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))
#     finally:
#         # Cleanup
#         if os.path.exists(json_path):
#             os.remove(json_path)
#
#     # STEP 2: Upload JSON file to R2
#     try:
#         bucket_url = r2.upload(output_path, bucket_path)
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Upload to R2 failed: {e}")
#
#     return {
#         "status": "success",
#         "records": record_count,
#         "json_file": json_path,
#         "r2_url": bucket_url
#     }
from datetime import date, datetime
from decimal import Decimal
import math

def make_json_safe(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    return obj

# @router.post("/parquet/read-save-mobile")
# def read_parquet_and_save_to_mobile(
#     path: str = Query(..., description="Full s3:// R2 parquet path"),
#     mobile: str = Query(..., description="Mobile number for bucket folder"),
#     limit: int = Query(1000, description="Rows to convert, 0 = full file")
# ):
#     """
#     Read parquet from R2 -> convert to JSON -> save JSON to R2 under mobile folder.
#     """
#     import pyarrow.parquet as pq
#     import json
#     import uuid
#     from io import BytesIO
#
#     try:
#         s3fs = get_s3_fs()
#
#         # Normalize s3://bucket/file.parquet
#         if path.startswith("s3://"):
#             path = path.replace("s3://", "", 1)
#
#         pq_file = pq.ParquetFile(path, filesystem=s3fs)
#
#         # Read batches safely
#         records = []
#         rows_read = 0
#
#         for batch in pq_file.iter_batches(batch_size=1000):
#             df = batch.to_pandas()
#             print("df",df)
#             df = df.applymap(make_json_safe)
#
#             part = df.to_dict(orient="records")
#
#             safe_part = []
#             for row in part:
#                 safe_row = {k: json_safe(v) for k, v in row.items()}
#                 safe_part.append(safe_row)
#
#             if limit > 0:
#                 # stop when limit reached
#                 remaining = limit - rows_read
#                 records.extend(part[:remaining])
#                 rows_read += len(part[:remaining])
#                 if rows_read >= limit:
#                     break
#             else:
#                 # full file
#                 records.extend(safe_part)
#
#         # Convert list -> JSON string
#         json_data = json.dumps(records, indent=2)
#         print("json:",json_data)
#         # Save directly to R2 bucket
#         r2 = get_r2_client()
#         bucket = "dev-transaction"
#
#         file_id = uuid.uuid4().hex[:10]
#         file_name = f"parquet_{file_id}.json"
#
#         key = f"mobile/{mobile}/parquet-json/{file_name}"
#
#         # Upload JSON to R2
#         r2.put_object(
#             Bucket=bucket,
#             Key=key,
#             Body=json_data.encode("utf-8"),
#             ContentType="application/json"
#         )
#
#         return {
#             "status": "success",
#             "source_parquet": path,
#             "mobile_store_path": f"r2://{bucket}/{key}",
#             "rows_written": len(records),
#             "total_rows_in_file": pq_file.metadata.num_rows
#         }
#
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Failed: {str(e)}")

@router.post("/parquet/read-save-mobile")
def read_parquet_and_save_to_mobile(
    path: str = Query(..., description="Full s3:// R2 parquet path"),
    # mobile: str = Query(..., description="Mobile number for bucket folder"),
    limit: int = Query(1000, description="Rows to convert (0 = full file)")
):
    """
    1. Reads parquet file from R2
    2. Converts rows to JSON-safe values
    3. Saves JSON output into R2 under mobile/<mobile>/parquet-json/
    """
    import pyarrow.parquet as pq
    import json
    import math
    from datetime import date, datetime
    from decimal import Decimal
    from collections import defaultdict

    try:
        s3fs = get_s3_fs()

        # Remove s3:// prefix
        if path.startswith("s3://"):
            path = path.replace("s3://", "", 1)

        pq_file = pq.ParquetFile(path, filesystem=s3fs)

        rows = []
        rows_read = 0

        # ----------- READ BATCHES SAFELY -----------
        for batch in pq_file.iter_batches(batch_size=1000):
            df = batch.to_pandas()

            # Convert every cell to JSON-safe value
            df = df.applymap(make_json_safe)

            part = df.to_dict(orient="records")

            # Limit rows if requested
            if limit > 0:
                remaining = limit - rows_read
                rows.extend(part[:remaining])
                rows_read += len(part[:remaining])
                if rows_read >= limit:
                    break
            else:
                rows.extend(part)

        mobile_groups = defaultdict(list)

        for row in rows:
            mobile = row.get("customer_mobile__c", "unknown")
            mobile = str(mobile).replace("/", "_").strip()
            mobile_groups[mobile].append(row)



        # ----------- CONVERT TO JSON STRING -----------
        json_data = json.dumps(rows, indent=2)

        # ----------- SAVE TO R2 BUCKET -----------
        r2 = get_r2_client()
        bucket = "dev-transaction"
        # print("data",json_data.get('customer_mobile__c'))
        saved_files = []

        for mobile, items in mobile_groups.items():
            key = f"mobile/{mobile}/{mobile}.json"
            json_body = json.dumps(items, indent=2)

            r2.put_object(
                Bucket=bucket,
                Key=key,
                Body=json_body.encode("utf-8"),
                ContentType="application/json"
            )

            saved_files.append(f"r2://{bucket}/{key}")

        return {
            "status": "success",
            "source_parquet": path,
            "unique_mobiles": list(mobile_groups.keys()),
            "files_saved": saved_files,
            "total_rows_processed": len(rows),
            "total_rows_in_file": pq_file.metadata.num_rows
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed: {str(e)}")