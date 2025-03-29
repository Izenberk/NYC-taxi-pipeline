from pyspark.sql import SparkSession

# Start Spark session with S3 configs
spark = SparkSession.builder \
    .appName("LoadCleanedToPostgres") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1") \
    .getOrCreate()

print("🚀 SparkSession ready!")

# Load cleaned data
clean_path = "s3a://nyc-taxi/clean/yellow_tripdata_2023-01-clean.parquet"
print("📥 Loading cleaned data from MinIO...")
df = spark.read.parquet(clean_path)

# Write to PostgreSQL
print("📤 Writing to PostgreSQL...")
jdbc_url = "jdbc:postgresql://postgres:5432/nyc_taxi"
db_properties = {
    "user": "admin",
    "password": "admin",
    "driver": "org.postgresql.Driver"
}

df.write \
  .format("jdbc") \
  .option("url", jdbc_url) \
  .option("dbtable", "yellow_tripdata_2023_01") \
  .option("user", "admin") \
  .option("password", "admin") \
  .option("driver", "org.postgresql.Driver") \
  .save()

print("✅ Data loaded into PostgreSQL successfully!")