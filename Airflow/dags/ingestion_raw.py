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
    # schedule_interval=None,
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
    valores["operacao"] = operacao

    return tabela, valores

def pull_to_MinIO(
        schema,
        tabela,
        files_source_table,
        file_format
    ):
    """
    pull all data to raw in MinIO
    """
    import os
    from include.minio_manager import MinioManager
    minio = MinioManager()
    
    for file in files_source_table:
        path = f'/{schema}/{tabela}'
        minio.upload_file('raw',path,file_format,file)
        os.remove(file)


def ingest_from_postgres():
    from include.postgres_manager import Postgres_Connector
    import json, os, glob
    root = f'{Variable.get("root_data")}/postgres_ingestion/cdc/'
    postgres = Postgres_Connector()
    postgres.open_connection()
    cursor = postgres.get_cursor()

    cursor.execute("SELECT * FROM pg_logical_slot_get_changes('python_slot', NULL, NULL);")
    changes = cursor.fetchall()

    if changes:
        for change in changes:
            lsn, xid, data = change
            resultado = parse_cdc_event(data)
            if resultado is not None:
                tabela, dados = resultado
                caminho = f'{root}{tabela}'
                os.makedirs(caminho,exist_ok=True)
                with open(f'{caminho}/{datetime.now().strftime('%y-%m-%d-%H-%M-%S-%f')}.json', "w", encoding="utf-8") as f:
                    json.dump(dados, f, indent=4, ensure_ascii=False)

    postgres.close_cursor(cursor)
    postgres.close_connection()

    tabelas = glob.glob(f'{root}*')
    for tabela_path in tabelas:
        files = glob.glob(f'{tabela_path}/*.json')
        tabela = tabela_path.split(root)[1]

        dados_combinados = []
        for file in files:
            with open(file, 'r', encoding='utf-8') as f:
                try:
                    dados = json.load(f)
                    dados_combinados.append(dados)
                except json.JSONDecodeError as e:
                    print(f"Erro ao ler {file}: {e}")


        output_file = os.path.join(tabela_path, "merged.json")
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(dados_combinados, f, ensure_ascii=False, indent=4)

        pull_to_MinIO('postgres/cdc/', tabela, [output_file], 'json')

        for file in files:
            os.remove(file)

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

        pull_to_MinIO('/postgres/full_load',tabela,glob.glob(f'{path}*.parquet'),'parquet')


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



