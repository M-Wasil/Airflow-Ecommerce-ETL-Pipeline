from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "Wasil",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id="ecommerce_etl_pipeline",
    description="ETL Pipeline for Books to Scrape",
    default_args=default_args,
    start_date=datetime(2026, 8, 1),
    schedule=None,
    catchup=False,
    tags=["ETL", "Books", "MongoDB"],
) as dag:

    extract_task = BashOperator(
        task_id="extract_data",
        bash_command="python /opt/airflow/scripts/extraction.py",
    )

    transform_task = BashOperator(
        task_id="transform_data",
        bash_command="python /opt/airflow/scripts/transform.py",
    )

    load_task = BashOperator(
        task_id="load_data",
        bash_command="python /opt/airflow/scripts/load.py",
    )

    extract_task >> transform_task >> load_task
