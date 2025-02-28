from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'spark',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1)
}

dag = DAG(
    dag_id='Populate_data',
    default_args=default_args,
    schedule_interval='*/5 * * * *',
    catchup=False,
    tags=['airflow','simulation']
) 

def populate_clientes(cursor):
    import random
    nomes = ["Ana", "Bruno", "Carlos", "Daniela", "Eduardo", "Fernanda", "Gabriel", "Helena", "Igor", "Juliana"]
    cidades = ["São Paulo", "Rio de Janeiro", "Belo Horizonte", "Porto Alegre", "Curitiba", "Recife", "Fortaleza", "Salvador", "Brasília", "Manaus"]

    for _ in range(100):
        nome = random.choice(nomes) + " " + random.choice(["Silva", "Santos", "Oliveira", "Souza", "Lima"])
        cidade = random.choice(cidades)

        cursor.execute("""
            INSERT INTO financeiro.clientes (nome, cidade)
            VALUES (?, ?)
        """, (nome, cidade))
    cursor.execute("SELECT max(cliente_id) from financeiro.clientes")
    numcli = int(cursor.fetchone()[0])
    for _ in range(10):
        
        nova_cidade = random.choice(cidades)
        cliente_id = random.randint(1, numcli)

        cursor.execute("""
            UPDATE financeiro.clientes
            SET cidade = ?
            WHERE cliente_id = ?
        """, (nova_cidade, cliente_id))

def populate_movimentos(cursor):
    import random
    from datetime import date, timedelta
    cursor.execute("SELECT max(cliente_id) from financeiro.clientes")
    numcli = int(cursor.fetchone()[0])
    for i in range(1000):
        data_movimento = date.today() - timedelta(days=random.randint(0, 365)) 
        valor = round(random.uniform(10, 5000), 2)
        cliente_id = random.randint(1, numcli)
        cursor.execute("""
            INSERT INTO financeiro.movimentos_financeiros (data_movimento, valor, cliente_id)
            VALUES (?, ?, ?)
        """, (data_movimento, valor, cliente_id))

def feed_postgres():
    from include.postgres_manager import Postgres_Connector
    postgres = Postgres_Connector()
    postgres.open_connection()
    cursor = postgres.get_cursor()

    populate_clientes(cursor)
    populate_movimentos(cursor)
    
    postgres.commit()
    postgres.close_cursor(cursor)
    postgres.close_connection()

feed_postgres = PythonOperator(
    task_id='feed_postgres',
    python_callable=feed_postgres,
    dag=dag
    )

feed_postgres