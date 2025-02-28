from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.models import Variable

default_args = {
    'owner': 'spark',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1)
}

dag = DAG(
    dag_id='Ingestion_raw',
    default_args=default_args,
    schedule_interval='*/10 * * * *',
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

def pull_to_MinIO(
        schema,
        tabela,
        files_source_table
    ):
    """
    pull all data to raw in MinIO
    """
    import os
    from include.minio_manager import MinioManager
    minio = MinioManager()
    
    for file in files_source_table:
        path = f'/{schema}/{tabela}'
        minio.upload_file('raw',path,'parquet',file)
        os.remove(file)


def ingest_from_postgres():
    from include.postgres_manager import Postgres_Connector
    import pandas as pd, os, glob, re

    postgres = Postgres_Connector()
    postgres.open_connection()
    cursor = postgres.get_cursor()

    cursor.execute("SELECT * FROM pg_logical_slot_get_changes('python_slot', NULL, NULL);")
    changes = cursor.fetchall()
    eventos = []
    if changes:
        
        for change in changes:
            lsn, xid, data = change
            dados = parse_cdc_event(data)
            if dados is not None:
                eventos.append(dados)
    postgres.close_cursor(cursor)
    postgres.close_connection()

    root = f'{Variable.get("root_data")}/postgres_ingestion/cdc/'
    if len(eventos) > 0:
        df = pd.DataFrame(eventos)
        os.makedirs(root,exist_ok=True)
        df.to_parquet(root,partition_cols=['tabela'],index=False)

    tabelas = glob.glob(f'{root}*')
    for tabela_path in tabelas:
        files = glob.glob(f'{tabela_path}/*.parquet')
        match = re.search(r'tabela=([^/]+)', tabela_path)
        tabela = ''
        if match:
            tabela = match.group(1)
            pull_to_MinIO('postgres/cdc/',tabela,files)

def full_load_postgres():
    from include.postgres_manager import Postgres_Connector
    import pandas as pd, os, glob
    postgres = Postgres_Connector()
    postgres.open_connection()
    cursor = postgres.get_cursor()

    root = f'{Variable.get("root_data")}/postgres_ingestion/full_load/'
    
    tabelas = ['financeiro.movimentos_financeiros','financeiro.clientes']
    for tabela in tabelas:
        date = datetime.now().strftime('%y-%m-%d-%H-%M-%S-%f')
        cursor.execute(f"SELECT * FROM {tabela}")
        data = cursor.fetchall()
        colunas = [desc[0] for desc in cursor.description]
        df = pd.DataFrame.from_records(data, columns=colunas)
        path = f'{root}{tabela}/'
        os.makedirs(path,exist_ok=True)
        df.to_parquet(f'{path}{date}.parquet',index=False)

        pull_to_MinIO('/postgres/full_load',tabela,glob.glob(f'{path}*.parquet'))


    postgres.close_cursor(cursor)
    postgres.close_connection()
    



ingest_from_postgres = PythonOperator(
    task_id='ingest_from_postgres',
    python_callable=ingest_from_postgres,
    dag=dag
    )

ingest_from_postgres


# full_load_postgres = PythonOperator(
#     task_id='full_load_postgres',
#     python_callable=full_load_postgres,
#     dag=dag
#     )


# full_load_postgres >> ingest_from_postgres

ingest_from_postgres



