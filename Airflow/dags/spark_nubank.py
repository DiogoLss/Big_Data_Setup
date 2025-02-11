from airflow import DAG
from datetime import datetime
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'spark',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1)
}

dag = DAG(
    dag_id='Spark_nubank',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['airflow','nubank']
) 

ingest_task = SparkSubmitOperator(
    task_id='spark_ingest_bronze',
    # application='/opt/airflow/spark/test.py',
    conn_id='spark_conn',
    verbose=False,
    total_executor_cores='1',
    executor_cores='1',
    executor_memory='1g',
    num_executors='1',
    driver_memory='1g',
    application='s3a://src/ingestao.py',
    # conf={
    #     "spark.hadoop.fs.s3a.endpoint": "host.docker.internal:9000",
    #     "spark.hadoop.fs.s3a.access.key": "HJxJREEYqvIz5WT8FCnf",
    #     "spark.hadoop.fs.s3a.secret.key": "hNUseilFYBukBOf6ZGZf53onYeR77ALK5NpDSOHZ",
    #     "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    #     "spark.hadoop.fs.s3a.path.style.access": "true",
    #     "spark.hadoop.fs.s3a.connection.ssl.enabled": "false"
    # },
    dag=dag
    # application_args=['arg1', 'arg2'], 
    )

ingest_task