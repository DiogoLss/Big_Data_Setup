from airflow import DAG
from datetime import datetime
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'spark',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1)
}

dag = DAG(
    dag_id='Postgres_spark',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['airflow','spark_jobs','postgres']
) 

ingest_extrato = SparkSubmitOperator(
    task_id='spark_ingest_extrato',
    application='',
    conn_id='spark_conn',
    application_args=[
        ''
    ],
    conf={
        'spark.driver.host':'airflow-worker'
    },
    dag=dag
    )

