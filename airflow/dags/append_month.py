from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

from tasks.warehouse import append_month

with DAG(
    dag_id="append_labeled_month",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    schedule="0 0 1 * *",
    catchup=False,
    max_active_runs=1,
) as dag:
    PythonOperator(task_id="append_month", python_callable=append_month)
