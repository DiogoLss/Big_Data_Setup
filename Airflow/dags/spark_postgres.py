from airflow import DAG
from datetime import datetime
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'spark',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1)
}

dag = DAG(
    dag_id='Spark_Postgres',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['postgres','spark']
)

ingest_financeiro_clientes = SparkSubmitOperator(
    task_id='ingest_financeiro_clientes',
    application='/src/bronze/ingestion.py',
    conn_id='spark_conn',
    application_args=[
        'postgres',
        'financeiro.clientes',
        'parquet',
        'true',
        'cliente_id',
        'modificado_em'
    ],
    conf={
        'spark.driver.host':'airflow-worker'
    },
    dag=dag
    )

ingest_financeiro_movimentos = SparkSubmitOperator(
    task_id='ingest_financeiro_movimentos',
    application='/src/bronze/ingestion.py',
    conn_id='spark_conn',
    application_args=[
        'postgres',
        'financeiro.movimentos_financeiros',
        'parquet',
        'true',
        'id',
        'modificado_em'
    ],
    conf={
        'spark.driver.host':'airflow-worker'
    },
    dag=dag
    )

ingest_silver_clientes = SparkSubmitOperator(
    task_id='ingest_silver_clientes',
    application='/src/silver/ingestion.py',
    conn_id='spark_conn',
    application_args=[
        'clientes'
    ],
    conf={
        'spark.driver.host':'airflow-worker'
    },
    dag=dag
    )

ingest_silver_movimentos = SparkSubmitOperator(
    task_id='ingest_silver_movimentos',
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

[ingest_financeiro_clientes, ingest_financeiro_movimentos]
ingest_financeiro_clientes >> ingest_silver_clientes
ingest_financeiro_movimentos >> ingest_silver_movimentos