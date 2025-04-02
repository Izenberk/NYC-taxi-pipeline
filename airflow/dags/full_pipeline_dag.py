from airflow import DAG
from airflow.operators.bash import BashOperator # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
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

        from ingest.download_parquet import main as download_main
        download_main()

    def upload_data():
        from pipeline.ingest.upload_to_minio import main as upload_main
        upload_main()

    download_to_minio = PythonOperator(
        task_id='download_data_parquet',
        python_callable=download_data
    )

    upload_to_minio = PythonOperator(
        task_id='upload_raw_to_minio',
        python_callable=upload_data
    )

    # 2️⃣ Clean raw data using Spark
    clean_data_spark = BashOperator(
        task_id='clean_data_spark',
        bash_command="""
        /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark-app/clean_yellow_tripdata.py
        """
    )

    # 3️⃣ Transform cleaned data into analytics
    transform_data_spark = BashOperator(
        task_id='transform_data_spark',
        bash_command="""
        /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark-app/transform_yellow_tripdata.py
        """
    )

    # 4️⃣ Load clean parquet to PostgreSQL
    load_to_postgres = BashOperator(
        task_id='load_to_postgres',
        bash_command="""
        /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark-app/load_to_postgres.py
        """
    )

    # 5️⃣ Run dbt models
    run_dbt_model = BashOperator(
        task_id='run_dbt_models',
        bash_command='cd /dbt/nyc_taxi_analytics && dbt run'
    )

    # 6️⃣ Run dbt tests
    run_dbt_test = BashOperator(
        task_id='run_dbt_tests',
        bash_command='cd /dbt/nyc_taxi_analytics && dbt test'
    )

    # 🔗 Define task dependencies
    download_to_minio >> upload_to_minio >> clean_data_spark >> transform_data_spark
    transform_data_spark >> load_to_postgres >> run_dbt_model >> run_dbt_test