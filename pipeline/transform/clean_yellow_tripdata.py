from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Start Spark session
spark = SparkSession.builder \
    .appName("CleanYellowTripdata") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1") \
    .getOrCreate()

print("✅ Spark session started")

# Load raw data from MinIO
raw_path = "s3a://nyc-taxi/raw/yellow_tripdata_2023-01.parquet"
print("📥 Reading raw data...")
df = spark.read.parquet(raw_path)

print("🧪 Raw schema:")
df.printSchema()

# Cleaning steps
print("🧼 Cleaning data...")
clean_df = df.filter(
    (col("tpep_pickup_datetime").isNotNull()) &
    (col("tpep_dropoff_datetime").isNotNull()) &
    (col("trip_distance") > 0) &
    (col("fare_amount") >= 0)
)

# Cast passenger_count to integer
clean_df = clean_df.withColumn("passenger_count", col("passenger_count").cast("integer"))

# Save cleaned data to MinIO
clean_path = "s3a://nyc-taxi/clean/yellow_tripdata_2023-01-clean.parquet"
print("📤 Writing cleaned data...")
clean_df.write.mode("overwrite").parquet(clean_path)

print("✅ Data cleaning complete and saved to MinIO!")