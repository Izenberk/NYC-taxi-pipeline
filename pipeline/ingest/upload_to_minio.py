from minio import Minio
from minio.error import S3Error

# Set up connection
client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

# Bucket and file info
bucket_name = "nyc-taxi"
object_name = "raw/yellow_tripdata_2023-01.parquet"
file_path = "data/raw/yellow_tripdata_2023-01.parquet"

# Make sure bucket exists
if not client.bucket_exists(bucket_name):
    print(f"🪣 Bucket '{bucket_name}' not found. Creating it...")
    client.make_bucket(bucket_name)

# Upload the file
print(f"☁️  Uploading {file_path} to {bucket_name}/{object_name}...")
client.fput_object(bucket_name, object_name, file_path)
print("✅ Upload to MinIO complete!")