import requests
import json
import pandas as pd
from datetime import date

def make_request(endpoint, headers, params = None):
    response = requests.get(endpoint, headers=headers, params=params)
    return json.loads(response.content)

def extract_stream_data(endpoint, headers, ts_nodash=None):
    stream_data = []
    
    # Set the initial pagination cursor to None
    pagination_cursor = None
    i = 0
    while True:
        # Set the query parameters and pagination cursor for the API request
        params = {
            "first": 100,
            "after": pagination_cursor
        }

        # Make a GET request to the Twitch API endpoint with the headers and query parameters
        response = make_request(endpoint, headers, params)
        stream_data += response["data"]
        
        # Check if there are more pages of streamer data to retrieve
        if "pagination" in response and "cursor" in response["pagination"]:
            pagination_cursor = response["pagination"]["cursor"]
        else:
            break
    
        if i == 10:
            break
        else: 
            i = i + 1

    
    with open('/opt/airflow/dags/data/streams_data.json', 'w') as fp:
        json.dump(stream_data, fp)    


def extract_game_data(headers, ts_nodash=None):
    #print(ts_nodash)
    with open('/opt/airflow/dags/data/streams_data.json', 'r') as fp:
        stream_data = json.load(fp)

    game_ids = [stream["game_id"] for stream in stream_data]
    games_data = []

    for i in range(0, len(game_ids), 100):
        games_endpoint = f"https://api.twitch.tv/helix/games?{'&'.join([f'id={game_id}' for game_id in game_ids[i:i+100]])}"
        games_data += make_request(games_endpoint, headers)["data"]


    with open('/opt/airflow/dags/data/games_data.json', 'w') as fp:
        json.dump(games_data, fp)


def extract_user_data(headers, ts_nodash=None):
    with open(f'/opt/airflow/dags/data/streams_data.json', 'r') as fp:
        stream_data = json.load(fp)
    
    user_ids = [stream["user_id"] for stream in stream_data]
    user_data = []

    for i in range(0, len(user_ids), 100):
            users_endpoint = f"https://api.twitch.tv/helix/users?{'&'.join([f'id={user_id}' for user_id in user_ids[i:i+100]])}"
            user_data += make_request(users_endpoint, headers)["data"]

    with open('/opt/airflow/dags/data/users_data.json', 'w') as fp:
        json.dump(user_data, fp)