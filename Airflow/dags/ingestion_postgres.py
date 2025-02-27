from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'spark',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1)
}

dag = DAG(
    dag_id='Ingest_From_Postgres',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['airflow','simulation','ingestion']
) 

def parse_cdc_event(data):
    import re
    """Extrai informações da string do CDC e converte para um dicionário estruturado."""
    match = re.match(r'table (\S+): (\w+): (.+)', data)
    if not match:
        return None

    tabela, operacao, colunas_raw = match.groups()
    colunas = re.findall(r'(\w+)\[', colunas_raw)
    valores_extraidos = re.findall(r":(?:'([^']*)'|(\d+(?:\.\d+)?))", colunas_raw)
    valores_final = [v[0] if v[0] else v[1] for v in valores_extraidos]
    valores = dict(zip(colunas, valores_final))
    valores["tabela"] = tabela
    valores["operacao"] = operacao

    return valores

def test():
    from include.postgres_manager import Postgres_Connector
    import json, pandas as pd
    postgres = Postgres_Connector()
    postgres.open_connection()
    cursor = postgres.get_cursor()

    cursor.execute("SELECT * FROM pg_logical_slot_get_changes('python_slot', NULL, NULL);")
    changes = cursor.fetchall()

    if changes:
        eventos = []
        for change in changes:
            lsn, xid, data = change
            dados = parse_cdc_event(data)
            if dados is not None:
                eventos.append(dados)
        df = pd.DataFrame(eventos)
        df.to_excel('/opt/airflow/dags/include/data/postgres_ingestion/test.xlsx',index=False)

    postgres.close_cursor(cursor)
    postgres.close_connection()

test = PythonOperator(
    task_id='ingest_to_minio',
    python_callable=test,
    dag=dag
    )

test



