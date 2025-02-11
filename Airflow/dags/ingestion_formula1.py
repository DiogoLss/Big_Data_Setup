from include.api.f1_project import get_data_from_api_year,get_data_from_api_per_season
from include.minio_manager import MinioManager
from datetime import datetime
from minio import Minio
import json, os
from airflow import DAG
from airflow.operators.python import PythonOperator

bucket_name = 'data'
path_root = '/opt/airflow/dags/include/data/f1_project'
endpoints = [
        'seasons',
        'results',
        'qualifying',
        'sprint',
        'driverStandings',
        'drivers',
        'constructors',
        'constructorStandings',
        'circuits',
        'status',
        'pitstops',
        'laps',
        'schedule'
    ]
#READ API

def save_to_raw(path,data):
    date = datetime.now().strftime('%y-%m-%d-%H-%M-%S')
    file_path = f'{path_root}/{path}/{date}.json'
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, 'w') as file:
        json.dump(data, file, indent=4)

def get_general_data(endpoint,year,offset):
    data = get_data_from_api_year(endpoint,year,offset)
    save_to_raw(endpoint,data)
    return data

def get_season_data(endpoint,year,season,offset):
    data = get_data_from_api_per_season(endpoint,year,season,offset)
    save_to_raw(endpoint,data)
    return data

def get_pages(endpoint,year,season=None):
    offset = 0
    next_page = 1000
    limit = 1000
    while True:
        if season == None:
            data = get_general_data(endpoint,year,offset)
        else:
            data = get_season_data(endpoint,year,season,offset)
        total = int(data["MRData"]["total"])
        if total > next_page:
            next_page += limit
            offset += limit
        else:
            break

def loop_api(endpoint,year_init = 1950):
    year_end = datetime.now().year + 1
    for year in range(year_init,year_end):
        if endpoint == 'laps' or endpoint == 'pitstops':
            if (year >= 1996 and endpoint == 'laps') or (year >= 2012 and endpoint == 'pitstops'):
                for season in range(1,25):
                    get_pages(endpoint,year,season)
        else:
            get_pages(endpoint,year)

def pull_data_to_raw(endpoint):
    #LOGIC OF WHICH YEAR BEGIN

    loop_api(endpoint)

def loop_endpoints():
    for endpoint in endpoints:
        print(f'getting data from {endpoint}')
        pull_data_to_raw(endpoint)

#SAVE TO BUCKET


def move_to_bucket():
    minio = MinioManager()
    for path in endpoints:
        path_folder = path_root + '/' + path
        path_to_save = 'raw/f1_data/' + path
        if os.path.exists(path_folder) and os.path.isdir(path_folder):
            print(path)
            for nome_arquivo in os.listdir(path_folder):
                caminho_completo = os.path.join(path_folder, nome_arquivo)
                if os.path.isfile(caminho_completo):
                    minio.upload_file(bucket_name,path_to_save,'json',caminho_completo)
                    os.remove(caminho_completo)


dag = DAG(
     dag_id="F1_Ingestion",
     start_date=datetime(2021, 1, 1),
     schedule=None,
     catchup=False
)

read_from_api = PythonOperator(
    task_id='read_save_from_api',
    python_callable=loop_endpoints,
    dag=dag
)

upload_files_to_bucket = PythonOperator(
    task_id='move_to_bucket',
    python_callable=move_to_bucket,
    dag=dag
)

read_from_api >> upload_files_to_bucket