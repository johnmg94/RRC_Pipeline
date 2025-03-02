from dotenv import load_dotenv
import os
import dotenv
import pandas as pd
import re
import sqlalchemy
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import numpy as np
import matplotlib.transforms as mtransforms
import matplotlib.ticker as ticker
import requests
import json

class Brent():
    def __init__(self):
        
        api_key_base = os.environ.get('FRED_API_KEY')
        self.api_key = '&api_key=' + str(api_key_base) + '&file_type=json'

    def search(self):
        search_url=r'https://api.stlouisfed.org/fred/series/search?search_text=real+gdp+us&'

        #search
        search_request = requests.get(search_url + self.api_key)
        json_item = json.loads(search_request.text)
        # print(json_item)
        json_item = json_item["seriess"]
        df_brent = pd.DataFrame(json_item)
        # print(df_brent.shape)
        # print("Brent")
        print(df_brent.head)
        print(df_brent.columns)
        print(df_brent.shape)
        print(df_brent.iloc[0])

        # for index,row in 

x = Brent()
x.search()