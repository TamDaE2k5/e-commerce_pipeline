from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from push_minio import push

with DAG(
    dag_id = 'push_data',
    start_date=datetime(2026, 1, 26),
    schedule='*/8 * * * *',
    tags=['push_MinIO'],
    catchup=False
) as dag:
    push = PythonOperator(
        task_id = 'push_data',
        python_callable=push
    )
    push