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

ingest_extrato = SparkSubmitOperator(
    task_id='spark_ingest_extrato',
    application='/src/bronze/ingestao.py',
    conn_id='spark_conn',
    application_args=[
        'nubank',
        'extrato',
        'csv',
        'true',
        "Data STRING, Valor DOUBLE, Identificador STRING, Descricao STRING",
        'None'
    ],
    conf={
        'spark.driver.host':'airflow-worker'
    },
    dag=dag
    )

ingest_fatura = SparkSubmitOperator(
    task_id='spark_ingest_fatura',
    application='/src/bronze/ingestao.py',
    conn_id='spark_conn',
    application_args=[
        'nubank',
        'fatura',
        'csv',
        'true',
        "date DATE, title STRING, amount DOUBLE",
        'None'
    ],
    conf={
        'spark.driver.host':'airflow-worker'
    },
    dag=dag
    )

[ingest_extrato, ingest_fatura]