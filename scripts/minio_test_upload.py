import boto3
from botocore.client import Config

# MinIO credentials
endpoint_url = "http://localhost:9000"
access_key = "minioadmin"
secret_key = "minioadmin"
bucket_name = "nyc-taxi"

# Create the S3 client
s3 = boto3.client(
    "s3",
    endpoint_url=endpoint_url,
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    config=Config(signature_version="s3v4"),
    region_name="ap-southeast-7" # Region doen't matter in MinIO
)

# Create a small test file
with open("test_file.txt", "w", encoding="utf-8") as f:
    f.write("Hello, MinIO")

# Upload it to your bucket
s3.upload_file("test_file.txt", bucket_name, "test/test_file.txt")

print("✅ Upload successful!")