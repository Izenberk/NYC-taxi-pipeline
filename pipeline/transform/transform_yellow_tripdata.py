from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum as _sum, count

# Start Spark session
spark = SparkSession.builder \
    .appName("TransformYellowTripdata") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1") \
    .getOrCreate()

print("✅ Spark session started")

# Load cleaned data
clean_path = "s3a://nyc-taxi/clean/yellow_tripdata_2023-01-clean.parquet"
print("📥 Reading cleaned data...")
df = spark.read.parquet(clean_path)

# Compute metrics
print("📊 Computing metrics...")

metrics_df = df.agg(
    count("*").alias("total_trips"),
    avg("trip_distance").alias("avg_trip_distance"),
    avg("fare_amount").alias("avg_fare_amount"),
    _sum("total_amount").alias("total_revenue")
)

# Save metrics to MinIO
output_path = "s3a://nyc-taxi/analytics/yellow_tripdata_2023-01-metrics.parquet"
print("💾 Saving metrics to MinIO...")
metrics_df.write.mode("overwrite").parquet(output_path)

print("✅ Metrics computation complete!")