def read_yellow_tripdata():
    from pyspark.sql import SparkSession

    print("🔥 Starting Spark session...")

    spark = SparkSession.builder \
        .appName("ReadYellowTripdataFromMinIO") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1") \
        .getOrCreate()

    print("✅ Spark session created!")

    try:
        print("📦 Trying to read Parquet file from MinIO...")
        df = spark.read.parquet("s3a://nyc-taxi/raw/yellow_tripdata_2023-01.parquet")
        print("✅ Data loaded successfully!")

        print("🧬 Schema:")
        df.printSchema()

        print("🔍 Sample rows:")
        df.show(5, truncate=False)

    except Exception as e:
        print("❌ Failed to read data!")
        print(e)

    finally:
        spark.stop()

if __name__=="__main__":
    read_yellow_tripdata()