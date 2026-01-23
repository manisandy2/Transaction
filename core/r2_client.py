import os
from dotenv import load_dotenv
import boto3
from botocore.client import Config
import logging
from fastapi import HTTPException


load_dotenv()
logger = logging.getLogger(__name__)
# Old version
class CloudflareR2Creds:
    def __init__(self):
        self.ACCOUNT_ID = os.getenv("ACCOUNT_ID")
        self.ACCESS_KEY_ID = os.getenv("ACCESS_KEY_ID")
        self.SECRET_ACCESS_KEY = os.getenv("SECRET_ACCESS_KEY")
        self.BUCKET_NAME = os.getenv("BUCKET_NAME")
        self.ENDPOINT = os.getenv("ENDPOINT")
        self.client = None

    def get_client(self):
        if not self.client:
            if not all([self.ACCESS_KEY_ID, self.SECRET_ACCESS_KEY, self.ENDPOINT]):
                raise ValueError("Missing Cloudflare R2 environment variables.")
        self.client = boto3.client(
            "s3",
            endpoint_url=self.ENDPOINT,
            aws_access_key_id=self.ACCESS_KEY_ID,
            aws_secret_access_key=self.SECRET_ACCESS_KEY,
            config=Config(signature_version="s3v4"),
            region_name="auto"
        )
        return self.client

# creds = CloudflareR2Creds()
# print(creds.get_client().list_buckets())

def get_r2_client():
    try:
        return CloudflareR2Creds().get_client()
    except Exception as e:
        logger.error(f"Failed to initialize R2 client: {e}")
        raise HTTPException(status_code=500, detail="Cloudflare R2 client initialization failed")

# New Version
# class CloudflareR2Creds:
#     def __init__(self):
#         self.ACCOUNT_ID = os.getenv("ACCOUNT_ID")
#         self.ACCESS_KEY_ID = os.getenv("ACCESS_KEY_ID")
#         self.SECRET_ACCESS_KEY = os.getenv("SECRET_ACCESS_KEY")
#         self.BUCKET_NAME = os.getenv("BUCKET_NAME")
#         self.ENDPOINT = os.getenv("ENDPOINT")
#         self.client = None
#
#     def get_client(self):
#         if self.client:  # already initialized
#             return self.client
#
#         # defensive: required vars
#         if not all([self.ACCESS_KEY_ID, self.SECRET_ACCESS_KEY, self.ENDPOINT]):
#             raise ValueError("Missing Cloudflare R2 env vars (ACCESS_KEY_ID / SECRET_ACCESS_KEY / ENDPOINT)")
#
#         # optional defensive: ACCOUNT ID must be part of endpoint
#         if self.ACCOUNT_ID and self.ACCOUNT_ID not in self.ENDPOINT:
#             raise ValueError("ENDPOINT does not contain ACCOUNT_ID. Example: https://<ACCOUNT_ID>.r2.cloudflarestorage.com")
#
#         self.client = boto3.client(
#             "s3",
#             endpoint_url=self.ENDPOINT,
#             aws_access_key_id=self.ACCESS_KEY_ID,
#             aws_secret_access_key=self.SECRET_ACCESS_KEY,
#             config=Config(signature_version="s3v4"),
#             region_name="auto"
#         )
#         return self.client
#
#
# def get_r2_client():
#     try:
#         return CloudflareR2Creds().get_client()
#     except Exception as e:
#         logger.error(f"Failed to initialize R2 client: {e}")
#         raise HTTPException(status_code=500, detail=f"Cloudflare R2 client init failed: {str(e)}")
