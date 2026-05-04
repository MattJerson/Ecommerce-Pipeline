FROM apache/airflow:2.8.0

USER airflow

RUN pip install --no-cache-dir dbt-postgres