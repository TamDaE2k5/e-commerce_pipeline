from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='spark_job',
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['spark', 'data_engineering']
) as dag:

    bronze_to_silver = BashOperator(
        task_id='bronze_to_silver',
        bash_command="""
        /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --deploy-mode client \
        --driver-memory 1g \
        --executor-memory 1g \
        --executor-cores 2 \
        --num-executors 2 \
        --conf spark.sql.shuffle.partitions=8 \
        --conf spark.network.timeout=600s \
        --conf spark.executor.heartbeatInterval=60s \
        --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
        --conf spark.hadoop.fs.s3a.access.key=minioadmin \
        --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
        --conf spark.hadoop.fs.s3a.path.style.access=true \
        --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
        --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
        --jars /opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
        /opt/airflow/scripts/br_to_sil.py
        """
    )

    silver_to_gold = BashOperator(
        task_id='silver_to_gold',
        bash_command="""
        /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --deploy-mode client \
        --driver-memory 1g \
        --executor-memory 1g \
        --executor-cores 2 \
        --num-executors 2 \
        --conf spark.sql.shuffle.partitions=8 \
        --conf spark.network.timeout=600s \
        --conf spark.executor.heartbeatInterval=60s \
        --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
        --conf spark.hadoop.fs.s3a.access.key=minioadmin \
        --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
        --conf spark.hadoop.fs.s3a.path.style.access=true \
        --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
        --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
        --jars /opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
        /opt/airflow/scripts/sil_to_gold.py
        """
    )

    clickhouse_init = BashOperator(
        task_id = 'init_db',
        bash_command='''
        cd /opt/airflow && \
        python scripts/clickhouse_init.py
        '''
    )

    insert_data = BashOperator(
        task_id = 'ins_data',
        bash_command='''
        cd /opt/airflow && \
        python scripts/gold_to_mart.py
        '''
    )


    bronze_to_silver >> silver_to_gold >> clickhouse_init >> insert_data
