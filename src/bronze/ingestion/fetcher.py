import requests
import json
from datetime import date
import builtins
import time

def fetch(url,base_params):
    all_data=[]
    page = 1
    retries = 0
    while True:
        if page > 4:
            break
        params = base_params.copy()
        params['page'] = page
        response = requests.get(url,params=params)
        if response.status_code == 429:
            retries += 1
            if retries > 5:
                raise Exception ("too many retries!")
            wait = builtins.min(60,2 ** retries)
            print(f"Rate limit hit, {retries} retry, wait {wait}")
            time.sleep(wait)
            continue

        if response.status_code != 200:
            raise Exception(f'fetch failed: {response.text}')

        data = response.json()
        all_data.extend(data)

        page += 1
        time.sleep(2)
        retries = 0
        print(page-1)

    return all_data