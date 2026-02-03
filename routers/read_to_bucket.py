


# import boto3
#


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