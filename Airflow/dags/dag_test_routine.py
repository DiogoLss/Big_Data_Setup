from airflow import DAG
from datetime import datetime
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'spark',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1)
}

dag = DAG(
    dag_id='test_routine',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['postgres']
)

ingest_financeiro_clientes = SparkSubmitOperator(
    task_id='ingest_financeiro_clientes',
    application='/src/silver/ingestion.py',
    conn_id='spark_conn',
    application_args=[
        'movimentos'
    ],
    conf={
        'spark.driver.host':'airflow-worker'
    },
    dag=dag
    )