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
    for attempt in range(3):  # Retry up to 3 times
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            print(f"Data fetched successfully: {response.status_code}")
            break
        elif response.status_code == 429: # Too Many Requests
            print("Rate limit exceeded. Retrying...")
            time.sleep(5)  # Wait before retrying
        else:
            raise Exception(f"HTTP error {response.status_code}")
    data = response.json()
    assessments = data.get('assessments', [])
    if not assessments:
        print("No more data available.")
        break
    for assessment in assessments:
        all_species.append( {
            "taxon_scientific_name" : assessment['taxon_scientific_name'], 
            "year_published" : assessment['year_published'], 
            "red_list_category_code" : assessment['red_list_category_code'], 
            "scopes" : assessment['scopes'][0]['description']['en'], 
            "latest" : assessment['latest']
        }
        )
    page += 1
    time.sleep(2)  # To respect API rate limits

df = pd.DataFrame(all_species)
pandas_gbq.to_gbq(df, "raw_ebird_daily.raw_iucn", project_id="daily-ebird", if_exists="replace", credentials=credentials)