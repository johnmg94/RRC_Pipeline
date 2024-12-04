import re
import json
from flask import jsonify
import requests
import pandas as pd
import os
import dotenv

# https://fredblog.stlouisfed.org/2020/05/wti-vs-brent-oil-prices-when-and-why-do-they-diverge/

# https://www.stlouisfed.org/on-the-economy/2014/april/the-divergence-of-spot-oil-prices

# https://fredblog.stlouisfed.org/2016/09/friction-in-oil-markets/

def fetch_series(base_url, api_key):
    url = str(base_url) + str(api_key)
    print("URL: ", url)
    try:
        r = requests.get(url)
    except Exception as e:
        print("Error: ", str(e))
    if r.status_code == 200:
        json_item = json.loads(r.text)
        try:
            json_item = json_item["observations"]
        except:
            print("'observations' key does not exist on json_item.")
            return json_item
        df = pd.DataFrame(json_item)
        print(df.head)
        print(df.columns)
        print(df.shape)

        # for index in range(len(df)):
        #     df.loc[index, 'id'] = str(index)
        # df = df.to_json(orient = "records")
        # json_load = json.loads(df)
        # response = json_load
        # return response
    else:
        status_code = { 'status_code_error': str(r.status_code)}
        return status_code

api_key_base = os.environ.get('FRED_API_KEY')
api_key = '&api_key=' + str(api_key_base) + '&file_type=json'
observation_start = '&realtime_start=1776-07-04&realtime_end=9999-12-31'
# frequency = '&frequency=m'
base_url = 'https://api.stlouisfed.org/fred/series/observations'
series = '?series_id=DCOILBRENTEU'
complete_base_url = base_url + series + observation_start
x = fetch_series(complete_base_url, api_key)
print(x)


series = '?series_id=DCOILWTICO'
complete_base_url = base_url + series + observation_start
y = fetch_series(complete_base_url, api_key)
print(x)
