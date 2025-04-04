def clean_yellow_tripdata(spark):
    from pyspark.sql.functions import col

    print("✅ Spark session received from DAG")

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

    clean_df = clean_df.withColumn("passenger_count", col("passenger_count").cast("integer"))

    # Save cleaned data to MinIO
    clean_path = "s3a://nyc-taxi/clean/yellow_tripdata_2023-01-clean.parquet"
    print("📤 Writing cleaned data...")
    clean_df.write.mode("overwrite").parquet(clean_path)

    print("✅ Data cleaning complete and saved to MinIO!")