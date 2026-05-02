FROM apache/airflow:3.1.8

USER airflow

RUN pip install --no-cache-dir \
    apache-airflow-providers-airbyte==5.4.1 \
    apache-airflow-providers-http \
    dbt-bigquery
