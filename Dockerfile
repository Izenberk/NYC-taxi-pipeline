FROM apache/airflow:2.8.1

USER airflow

RUN pip install dbt-core dbt-postgres