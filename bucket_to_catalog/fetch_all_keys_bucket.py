import boto3, os, time
from dotenv import load_dotenv
from botocore.client import Config

load_dotenv(".env")


def r2_client():
    try:
        return boto3.client("s3",
                            endpoint_url=os.getenv("ENDPOINT"),
                            aws_access_key_id=os.getenv("ACCESS_KEY_ID"),
                            aws_secret_access_key=os.getenv("SECRET_ACCESS_KEY"),
                            config=Config(signature_version="s3v4"),
                            region_name="auto"
                            )
    except Exception as e:
        print(f"\n❌ Failed to initialize R2 client: {e}")


def list_json_files(bucket: str, prefix: str):
    start = time.time()
    r2 = r2_client()
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

    print(f"\n---> 📂 Total Keys '{len(keys)}' retrieve in {time.time() - start, 2}ms\n")
    return keys


# BUCKET = "pos-transaction-imei"
# # PREFIX = "mobile/9148215410/transferred/"
# PREFIX = "imei/354393356305954/id/"


# list_json_files(bucket=BUCKET, prefix=PREFIX)

if __name__ == "__main__":
    BUCKET = "pos-transaction-imei"
    # # PREFIX = "mobile/9148215410/transferred/"
    PREFIX = "history/2026/03/04/"
    list_json_files(bucket=BUCKET, prefix=PREFIX)