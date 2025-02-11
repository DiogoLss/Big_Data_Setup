import requests

root_url = 'https://ergast.com/api/f1'
file_format = 'json'

def get_data_from_api_year(endpoint,year,offset,limit=1000):
    url = ''
    if endpoint == 'schedule':
        url = f'{root_url}/{year}.{file_format}'
    else:
        url = f'{root_url}/{year}/{endpoint}.{file_format}'
    params = {
        'offset':offset,
        'limit':limit
    }
    response = requests.get(url,params=params)
    return response.json()

def get_data_from_api_per_season(endpoint,year,season,offset,limit=1000):
    url = f'{root_url}/{year}/{season}/{endpoint}.{file_format}'
    params = {
        'offset':offset,
        'limit':limit
    }
    response = requests.get(url,params=params)
    return response.json()