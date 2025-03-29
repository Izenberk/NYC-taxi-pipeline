from pyspark.sql import SparkSession


# Create Spark session
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

# Remote Parquet URL (January 2023 Yellow Taxi)
s3_path = "s3a://nyc-taxi/raw/yellow_tripdata_2023-01.parquet"

# Read Parquet file directly from URL
df = spark.read.parquet(s3_path)

# Show schema and few rows
df.printScema()
df.show(5)

# Stop Spark session
spark.stop()
