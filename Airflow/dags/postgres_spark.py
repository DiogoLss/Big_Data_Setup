from airflow import DAG
from datetime import datetime
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'spark',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1)
}

dag = DAG(
    dag_id='Spark_Jobs',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['postgres']
)

ingest_extrato = SparkSubmitOperator(
    task_id='ingest_extrato',
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
    task_id='ingest_fatura',
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

nubank_to_silver = SparkSubmitOperator(
    task_id='nubank_to_silver',
    application='/src/silver/nubank.py',
    conn_id='spark_conn',
    conf={
        'spark.driver.host':'airflow-worker'
    },
    dag=dag
    )

nubank_to_gold = SparkSubmitOperator(
    task_id='nubank_to_gold',
    application='/src/gold/nubank.py',
    conn_id='spark_conn',
    conf={
        'spark.driver.host':'airflow-worker'
    },
    dag=dag
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
        'financeiro.clientes',
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


[ingest_extrato, ingest_fatura] >> nubank_to_silver >> nubank_to_gold
[ingest_financeiro_clientes, ingest_financeiro_movimentos]