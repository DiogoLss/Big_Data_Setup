from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'spark',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1)
}

dag = DAG(
    dag_id='Ingest_Postgres',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['airflow','simulation','ingestion']
) 

def test():
    import pyodbc,json
    conn_str = (
            "DRIVER={PostgreSQL Unicode};"
            "SERVER=postgres_simulation;"
            "PORT=5432;"
            "DATABASE=simulation;"
            "UID=postgres;"
            "PWD=postgres;"
        )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()



    cursor.execute("SELECT * FROM pg_logical_slot_get_changes('python_slot', NULL, NULL);")
    changes = cursor.fetchall()

    if changes:
            eventos = []
            for change in changes:
                lsn, xid, data = change
                eventos.append(json.loads(data)) 
            print(eventos)

test = PythonOperator(
    task_id='ingest_to_minio',
    python_callable=test,
    dag=dag
    )

test



