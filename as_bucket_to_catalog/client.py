import aioboto3
from botocore.config import Config
from dotenv import load_dotenv
import os
load_dotenv(".env")

def get_async_r2_client():
    return aioboto3.Session().client(
        "s3",
        endpoint_url=os.getenv("ENDPOINT"),
        aws_access_key_id=os.getenv("ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("ACCESS_KEY_ID"),
        config=Config(signature_version="s3v4"),
        region_name="auto",
    )