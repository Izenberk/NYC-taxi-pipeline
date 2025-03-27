from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
    .appName("NYC Taxi Ingestion") \
    .getOrCreate()

# Remote Parquet URL (January 2023 Yellow Taxi)
parquet_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"

# Read Parquet file directly from URL
df = spark.read.parquet(parquet_url)

# Show schema and few rows
df.printScema()
df.show(5)

# Stop Spark session
spark.stop()
