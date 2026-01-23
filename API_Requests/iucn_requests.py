import requests
import pandas as pd
import time
import pandas_gbq
from google.oauth2 import service_account
import os
import json
import base64

# Set up Google BigQuery credentials
credentials_b64 = os.environ["BQ_SERVICE_ACCOUNT"]
credentials_info = base64.b64decode(credentials_b64).decode("utf-8")
credentials_json = json.loads(credentials_info)
credentials = service_account.Credentials.from_service_account_info(credentials_json)
# IUCN Red List API request to get all bird species
api_key = os.environ["IUCN_APIKEY"]
headers = {"accept": "application/json", "Authorization": api_key}

# Initialize empty list to store all species data as they are split across pages
all_species = []
page = 1

while True: 
    url = "https://api.iucnredlist.org/api/v4/taxa/class/aves"
    params = {
        "page": page,
        "per_page": 100,
        }
    response = requests.get(url, headers=headers, params=params)
    if response.status_code != 200:
        print(f"Error fetching data: {response.status_code}")
        break
    data = response.json()
    if not data['assessments']:
        print("No more data available.")
        break
    for assessment in data['assessments']:
        data_dic = {
            "taxon_scientific_name" : assessment['taxon_scientific_name'], 
            "year_published" : assessment['year_published'], 
            "red_list_category_code" : assessment['red_list_category_code'], 
            "scopes" : assessment['scopes'][0]['description']['en'], 
            "latest" : assessment['latest']
        }
    df = pd.json_normalize(data_dic)
    # keep only relevant columns : scientific name, year of the assessment and the red list category
    df = df[["taxon_scientific_name", "year_published", "red_list_category_code"]]
    all_species.append(df)
    page += 1
    time.sleep(0.5)  # To respect API rate limits

df_iucn = pd.concat(all_species, ignore_index=True)
df_iucn.head()
pandas_gbq.to_gbq(df_iucn, "raw_ebird_daily.raw_iucn", project_id="daily-ebird", if_exists="replace", credentials=credentials)
