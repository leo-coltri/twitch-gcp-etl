import os
import requests
import json
import re
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from scripts import data_extraction

default_args = {
    'owner': 'leo-coltri',
    'depends_on_past': False,
    'catchup': False,
    'start_date': datetime(2023, 4, 1),
}

with DAG(
    dag_id="twitch_data_pipeline",
    schedule_interval="@yearly",
    default_args=default_args,
) as dag:

    authURL = 'https://id.twitch.tv/oauth2/token'
    
    ## Put your Twitch Credentials here!!
    client_ID = '' 
    secret = ''

    AutParams = {
        'client_id': client_ID,
        'client_secret': secret,
        'grant_type': 'client_credentials'
    }

    AutCall = requests.post(url=authURL, params=AutParams)
    access_token = AutCall.json()['access_token']

    headers = {
        'Client-ID': client_ID,
        'Authorization': f"Bearer {access_token}"
    }
    
    top_streams_endpoint = "https://api.twitch.tv/helix/streams?first=100"

    ext_stream_data = PythonOperator(
        task_id='extract_stream_data',
        provide_context=True,
        python_callable=data_extraction.extract_stream_data,
        op_kwargs={'endpoint': top_streams_endpoint, 'headers': headers},
        dag=dag,
    )

    ext_game_data = PythonOperator(
        task_id='extract_game_data',
        provide_context=True,
        python_callable=data_extraction.extract_game_data,
        op_kwargs={'headers': headers},
        dag=dag,
    )

    ext_user_data = PythonOperator(
        task_id='extract_user_data',
        provide_context=True,
        python_callable=data_extraction.extract_user_data,
        op_kwargs={'headers': headers},
        dag=dag,
    )

    ext_stream_data >> [ext_game_data, ext_user_data]