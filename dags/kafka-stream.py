from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'KarolR',
    'start_date': datetime(2024, 1, 1, 1)
}

def get_data():
    import json
    import requests

    res = requests.get('https://jsonplaceholder.typicode.com/posts/')
    print(res.json())
    #res = res['results'][0]
    print(res)
    return res

def format_data(res):
    data = {}
    data['first name'] = res['name']
    data['last name'] = res['name']['last']

def stream_data():
    import json
    res = get_data()
    res = format_data(res)
    print(json.dumps(res, indent=3))

with DAG(
    'user_automation',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )

get_data()
print('Hello there!')