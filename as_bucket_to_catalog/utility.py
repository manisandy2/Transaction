from as_bucket_to_catalog.client import get_async_r2_client
import json
import asyncio
import gc
import pyarrow as pa
from memory_monitor import log_memory


async def iter_json_files_async(bucket: str, prefix: str):
    async with get_async_r2_client() as client:

        continuation_token = None

        while True:
            params = {
                "Bucket": bucket,
                "Prefix": prefix,
                "MaxKeys": 500   # smaller = lower memory
            }

            if continuation_token:
                params["ContinuationToken"] = continuation_token

            response = await client.list_objects_v2(**params)

            contents = response.get("Contents", [])

            for item in contents:
                key = item["Key"]
                if not key.endswith("/"):
                    yield key

            if not response.get("IsTruncated"):
                break

            continuation_token = response.get("NextContinuationToken")

async def download_json_async(client, bucket, key):
    response = await client.get_object(Bucket=bucket, Key=key)

    async with response["Body"] as stream:
        data = await stream.read()   # You can also stream chunk-by-chunk

    return json.loads(data)

async def process_files_async(bucket, prefix, table, arrow_schema):

    semaphore = asyncio.Semaphore(10)  # 🔥 control concurrency
    total_rows = 0

    async with get_async_r2_client() as client:

        async for key in iter_json_files_async(bucket, prefix):

            await semaphore.acquire()

            asyncio.create_task(
                handle_file(
                    client,
                    bucket,
                    key,
                    table,
                    arrow_schema,
                    semaphore
                )
            )

    return total_rows

async def handle_file(client, bucket, key, table, arrow_schema, semaphore):
    try:
        log_memory(f"BEFORE download {key}")
        rows = await download_json_async(client, bucket, key)

        if not rows:
            return
        log_memory(f"AFTER download {key}")
        arrow_table = pa.Table.from_pylist(rows, schema=arrow_schema)
        log_memory(f"AFTER arrow build {key}")
        table.append(arrow_table)
        log_memory(f"AFTER append {key}")
        del rows
        del arrow_table
        gc.collect()
        pa.default_memory_pool().release_unused()
        log_memory(f"AFTER release {key}")
    finally:
        semaphore.release()