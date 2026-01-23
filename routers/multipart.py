from fastapi import APIRouter
from core.r2_client import get_r2_client

router = APIRouter(prefix="", tags=["Multipart"])

@router.post("/multipart/abort-all")
def abort_all_multipart_uploads(bucket_name: str):
    r2_client = get_r2_client()

    paginator = r2_client.get_paginator("list_multipart_uploads")

    aborted = []
    total = 0

    for page in paginator.paginate(Bucket=bucket_name):

        uploads = page.get("Uploads", [])
        for u in uploads:
            key = u["Key"]
            upload_id = u["UploadId"]
            r2_client.abort_multipart_upload(
                Bucket=bucket_name,
                Key=key,
                UploadId=upload_id
            )
            total += 1
            aborted.append({"key": key, "upload_id": upload_id})

    return {
        "bucket": bucket_name,
        "aborted_uploads": total,
        "details": aborted
    }

@router.delete("/multipart/abort-all")
def abort_all_multipart_uploads(bucket_name: str):
    r2_client = get_r2_client()
    paginator = r2_client.get_paginator("list_multipart_uploads")

    aborted = []
    total = 0

    for page in paginator.paginate(Bucket=bucket_name):
        uploads = page.get("Uploads", [])
        for u in uploads:
            key = u["Key"]
            upload_id = u["UploadId"]

            # abort incomplete upload
            r2_client.abort_multipart_upload(
                Bucket=bucket_name,
                Key=key,
                UploadId=upload_id
            )

            # delete any partial object if exists
            try:
                r2_client.delete_object(Bucket=bucket_name, Key=key)
            except Exception as delete_err:
                print(f"Failed to delete key: {key} - {delete_err}")
                pass

            total += 1
            aborted.append({"key": key, "upload_id": upload_id})

    return {
        "bucket": bucket_name,
        "aborted_uploads": total,
        "status": "multipart aborted + objects deleted",
        "details": aborted
    }