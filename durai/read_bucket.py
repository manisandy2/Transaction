# Removed dead code
import time
from core.r2_client import get_r2_client


def list_json_files(bucket: str, prefix: str):
    start = time.time()
    r2 = get_r2_client()
    keys = []

    token = None

    while True:
        params = {
            "Bucket": bucket,
            "Prefix": prefix
        }

        if token:
            params["ContinuationToken"] = token

        resp = r2.list_objects_v2(**params)

        for item in resp.get("Contents", []):
            key = item["Key"]
            if not key.endswith("/"):
                keys.append(key)

        if not resp.get("IsTruncated"):
            break

        token = resp.get("NextContinuationToken")

    print("Total keys:", len(keys))
    print("Unique keys:", len(set(keys)))  # 🔥 IMPORTANT

    return list(set(keys))  # REMOVE DUPLICATES

if __name__ == "__main__":
    list_json_files(prefix="history/2026/01/29/",bucket="pos-transaction-imei")