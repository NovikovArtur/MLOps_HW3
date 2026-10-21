from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

from tasks.quality import check_quality

with DAG(
    dag_id="check_data_quality",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    schedule="0 1 * * *",
    catchup=False,
    max_active_runs=1,
) as dag:
    PythonOperator(task_id="check_quality", python_callable=check_quality, op_kwargs={"run_id": "{{ run_id }}"})
