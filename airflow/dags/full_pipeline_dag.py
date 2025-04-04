from airflow import DAG
from airflow.operators.bash import BashOperator # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

# 📆 Schedule: run daily
with DAG(
    dag_id='full_pipeline_dag',
    default_args=default_args,
    description='End-to-End NYC Yellow Taxi Data Pipeline',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['nyc-taxi', 'end-to-end']
) as dag:
    
    # 1️⃣ Ingest and upload raw data to MinIO
    def download_data():
        import sys
        sys.path.insert(0, "/opt/pipeline")  # 💡 make it importable

        from ingest.download_data import download_parquet
        download_parquet()

    def upload_data():
        import sys
        sys.path.insert(0, "/opt/pipeline")  # 👈 Make sure Python sees your mounted package

        from ingest.upload_to_minio import upload_tripdata_to_minio  # 👈 assuming this is your function
        upload_tripdata_to_minio()

    download_to_minio = PythonOperator(
        task_id='download_data_parquet',
        python_callable=download_data
    )

    upload_to_minio = PythonOperator(
        task_id='upload_raw_to_minio',
        python_callable=upload_data
    )

    # 2️⃣ Clean raw data using Spark
    clean_data_spark = SparkSubmitOperator(
        task_id='clean_data_spark',
        application='/opt/spark-app/clean_yellow_tripdata.py',
        conn_id='spark_default',
        verbose=True,
        conf={
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.access.key": "minioadmin",
            "spark.hadoop.fs.s3a.secret.key": "minioadmin",
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        },
        jars=(
        "/opt/spark-extra-jars/hadoop-aws-3.3.6.jar,"
        "/opt/spark-extra-jars/aws-java-sdk-bundle-1.12.696.jar,"
        "/opt/spark-extra-jars/hadoop-common-3.3.6.jar"
    ),
)


    # 3️⃣ Transform cleaned data into analytics
    transform_data_spark = SparkSubmitOperator(
    task_id='transform_data_spark',
    application='/opt/spark-app/transform_yellow_tripdata.py',
    conn_id='spark_default',
    verbose=True,
    conf={
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.access.key": "minioadmin",
        "spark.hadoop.fs.s3a.secret.key": "minioadmin",
        "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    },
    jars=(
        "/opt/spark-extra-jars/hadoop-aws-3.3.6.jar,"
        "/opt/spark-extra-jars/aws-java-sdk-bundle-1.12.696.jar,"
        "/opt/spark-extra-jars/hadoop-common-3.3.6.jar"
    ),
)

    # 4️⃣ Load clean parquet to PostgreSQL
    load_to_postgres = SparkSubmitOperator(
    task_id='load_to_postgres',
    application='/opt/spark-app/load_to_postgres.py',
    conn_id='spark_default',
    verbose=True,
    conf={
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.access.key": "minioadmin",
        "spark.hadoop.fs.s3a.secret.key": "minioadmin",
        "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    },
    jars=(
        "/opt/spark-extra-jars/hadoop-aws-3.3.6.jar,"
        "/opt/spark-extra-jars/aws-java-sdk-bundle-1.12.696.jar,"
        "/opt/spark-extra-jars/hadoop-common-3.3.6.jar,"
        "/opt/spark-extra-jars/postgresql-42.7.5.jar"  # 🧩 JDBC driver for PostgreSQL
    ),
)


    # 5️⃣ Run dbt models
    run_dbt_staging = BashOperator(
        task_id='run_dbt_staging',
        bash_command='cd /dbt/nyc_taxi_analytics && dbt run --select staging',
    )

    run_dbt_base = BashOperator(
        task_id='run_dbt_base',
        bash_command='cd /dbt/nyc_taxi_analytics && dbt run --select base',
    )


    # 6️⃣ Run dbt tests
    run_dbt_test = BashOperator(
        task_id='run_dbt_tests',
        bash_command='cd /dbt/nyc_taxi_analytics && dbt test'
    )

    # 🔗 Define task dependencies
    download_to_minio >> upload_to_minio >> clean_data_spark >> transform_data_spark
    transform_data_spark >> load_to_postgres >> run_dbt_staging >> run_dbt_base >> run_dbt_test