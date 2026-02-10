from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='data_mart',
    schedule=None,
    catchup=False,
) as dag:
    clickhouse_init = BashOperator(
        task_id = 'ins_data',
        bash_command='''
        cd /opt/airflow && \
        python scripts/gold_to_mart.py
        '''
    )

    clickhouse_init