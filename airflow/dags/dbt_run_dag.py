from airflow import DAG
from airflow.operators.bash_operator import BashOperator # type: ignore
from datetime import datetime, timedelta

default_args = {
    'owner': 'bb',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='run_dbt_models',
    default_args=default_args,
    schedule_interval=None, # manual trigger for now
    catchup=False,
    tags=['dbt', 'nyc-taxi'],
    description='Run all dbt models from CLI inside container',
) as dag:
    
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/airflow/dbt/nyc_taxi_analytics && dbt run',
    )