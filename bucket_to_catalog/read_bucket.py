# Removed dead code
import time
from core.r2_client import get_r2_client
from core.catalog_client import get_catalog_client
from datetime import datetime, timedelta
import pyarrow as pa

# def list_json_files(bucket: str, prefix: str):
#     start = time.time()
#     r2 = get_r2_client()
#     keys = []
#
#     token = None
#
#     while True:
#         params = {
#             "Bucket": bucket,
#             "Prefix": prefix
#         }
#
#         if token:
#             params["ContinuationToken"] = token
#
#         resp = r2.list_objects_v2(**params)
#
#         for item in resp.get("Contents", []):
#             key = item["Key"]
#             if not key.endswith("/"):
#                 keys.append(key)
#
#         if not resp.get("IsTruncated"):
#             break
#
#         token = resp.get("NextContinuationToken")
#
#     print("Total keys:", len(keys))
#     print("Unique keys:", len(set(keys)))  # 🔥 IMPORTANT
#
#     return list(set(keys))  # REMOVE DUPLICATES

def list_json_files(bucket: str, prefix: str):
    start = time.time()
    r2 = get_r2_client()

    keys = set()   # use set directly (no duplicate list → set conversion)
    token = None

    while True:
        params = {
            "Bucket": bucket,
            "Prefix": prefix,
            "MaxKeys": 1000  # S3 page limit
        }

        if token:
            params["ContinuationToken"] = token

        resp = r2.list_objects_v2(**params)

        for item in resp.get("Contents", []):
            key = item["Key"]
            if not key.endswith("/"):
                keys.add(key)   # directly add to set

        if not resp.get("IsTruncated"):
            break

        token = resp.get("NextContinuationToken")

    print("Total keys:", len(keys))
    print(f"Time taken: {time.time() - start:.2f}s")

    return list(keys)

# if __name__ == "__main__":
#     list_json_files(prefix="history/2026/01/29/",bucket="pos-transaction-imei")

def update_last_pri_id(namespace: str, table_name: str, new_value: int):

    catalog = get_catalog_client()
    identifier = f"{namespace}.ingestion_tracking"

    tracking_table = catalog.load_table(identifier)

    arrow_schema = pa.schema([
        pa.field("namespace", pa.string(), nullable=False),
        pa.field("table_name", pa.string(), nullable=False),
        pa.field("high_watermark", pa.string(), nullable=False),
        pa.field("updated_at", pa.timestamp("us"), nullable=False),
    ])

    data = [{
        "namespace": namespace,
        "table_name": table_name,
        "high_watermark": str(new_value),
        "updated_at": datetime.utcnow()
    }]

    arrow_table = pa.Table.from_pylist(data, schema=arrow_schema)

    tracking_table.append(arrow_table)

    print(f"✅ High watermark updated → {table_name} = {new_value}")

def get_last_column_name(namespace, table_name,column_name):
    catalog = get_catalog_client()
    table = catalog.load_table(f"{namespace}.{table_name}")

    try:
        result = (
            table.scan(selected_fields=[column_name])
                 .to_arrow()[column_name]
                 .to_pylist()
        )
        print(len(result))
        print(max(result))
        print("increment",max(result)+1)
        return max(result) if result else 0
    except Exception :
        return 0


# get_last_pri_id("POS_Transactions", "Transaction_vars")

def iter_json_files(bucket: str, prefix: str):
    r2 = get_r2_client()
    token = None

    while True:
        params = {
            "Bucket": bucket,
            "Prefix": prefix,
            "MaxKeys": 1000
        }

        if token:
            params["ContinuationToken"] = token

        resp = r2.list_objects_v2(**params)

        for item in resp.get("Contents", []):
            key = item["Key"]
            if not key.endswith("/"):
                yield key

        if not resp.get("IsTruncated"):
            break

        token = resp.get("NextContinuationToken")


def get_last_value(namespace: str, table_name: str) -> int:
    """
    Get latest high_watermark for a table from ingestion_tracking.
    Returns 0 if not found.
    """

    catalog = get_catalog_client()
    identifier = f"{namespace}.ingestion_tracking"

    try:
        tracking_table = catalog.load_table(identifier)
    except Exception:
        return 0

    try:
        # Filter only this table
        scan = tracking_table.scan(
            row_filter=f"table_name = '{table_name}'",
            selected_fields=["high_watermark"]
        ).to_arrow()

        if scan.num_rows == 0:
            return 0

        values = scan["high_watermark"].to_pylist()

        # Convert to int and return max
        return max(int(v) for v in values if v is not None)

    except Exception:
        return 0