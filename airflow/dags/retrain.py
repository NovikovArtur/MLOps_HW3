from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

from tasks.retraining import prepare_window, publish_model, train_model

with DAG(
    dag_id="retrain_fraud_model",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
) as dag:
    prepare = PythonOperator(task_id="prepare_dataset", python_callable=prepare_window, op_kwargs={"run_id": "{{ run_id }}"})
    train = PythonOperator(task_id="train_model", python_callable=train_model, op_kwargs={"stage": "{{ ti.xcom_pull(task_ids='prepare_dataset') }}"})
    publish = PythonOperator(task_id="publish_model", python_callable=publish_model, op_kwargs={"candidate_path": "{{ ti.xcom_pull(task_ids='train_model') }}"})

    prepare >> train >> publish
