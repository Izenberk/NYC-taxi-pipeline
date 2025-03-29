import boto3
from botocore.client import Config
import os

endpoint_url = "http://localhost:9000"
access_key = "minioadmin"
secret_key = "minioadmin"
bucket_name = "nyc-taxi"
s3_key = "raw/yellow_tripdata_2023-01.parquet"
local_file = "data/raw/yellow_tripdata_2023-01.parquet"

s3 = boto3.client(
    "s3",
    endpoint_url=endpoint_url,
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    config=Config(signature_version="s3v4"),
    region_name="us-east-1"
)

if os.path.exists(local_file):
    print(f"Uploading {local_file} to MinIO...")
    s3.upload_file(local_file, bucket_name, s3_key)
    print("Upload complete!")
else:
    print("File not found! Did you download it?")