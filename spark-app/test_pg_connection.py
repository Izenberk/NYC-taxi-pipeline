from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("PostgresConnectionTest") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1") \
    .config("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.7.5.jar") \
    .getOrCreate()

print("🚀 SparkSession ready!")

# Sample connection test — show existing tables
df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/nyc_taxi") \
    .option("dbtable", "information_schema.tables") \
    .option("user", "admin") \
    .option("password", "admin") \
    .option("driver", "org.postgresql.Driver") \
    .load()

print("✅ Finish loading")
df.show(5)
