from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import boto3
from botocore.exceptions import ClientError

def upload_tripdata_to_minio():
    
    # Setup S3 hook using Airflow connection (Conn ID = minio_conn)
    hook = S3Hook(aws_conn_id="minio_conn")

    # File and bucket info
    bucket_name = "nyc-taxi"
    object_name = "raw/yellow_tripdata_2023-01.parquet"
    file_path = "/opt/data/raw/yellow_tripdata_2023-01.parquet"

    # Use boto3 client from S3Hook
    s3 = hook.get_conn()

    # Ensure the bucket exists
    try:
        s3.head_bucket(Bucket=bucket_name)
        print(f"🪣 Bucket '{bucket_name}' already exists.")
    except ClientError:
        print(f"🆕 Bucket '{bucket_name}' not found. Creating...")
        s3.create_bucket(Bucket=bucket_name)
        print(f"✅ Bucket '{bucket_name}' created.")

    # Upload file
    print(f"☁️ Uploading {file_path} to s3://{bucket_name}/{object_name}")
    hook.load_file(
        filename=file_path,
        key=object_name,
        bucket_name=bucket_name,
        replace=True
    )
    print("✅ Upload complete using S3Hook!")

if __name__ == "__main__":
    upload_tripdata_to_minio()