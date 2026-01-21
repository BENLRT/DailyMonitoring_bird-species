# import necessary libraries
import requests
import pandas as pd 
import pandas_gbq
from google.oauth2 import service_account
import time
import os
import json
import base64
from datetime import datetime, timedelta

# define API key and base URL for eBird API
api_key = os.environ["EBIRD_APIKEY"]
headers = {"X-eBirdApiToken": api_key}

# Set up Google BigQuery credentials
credentials_b64 = os.environ["BQ_SERVICE_ACCOUNT"]
print("DEBUG BQ_SERVICE_ACCOUNT_B64 length:", len(credentials_b64))

credentials_info = base64.b64decode(credentials_b64).decode("utf-8")
credentials_json = json.loads(credentials_info)
credentials = service_account.Credentials.from_service_account_info(credentials_json)

# initialize empty DataFrame to store all results
df_all=pd.DataFrame()

# calculate yesterday's date
yesterday = datetime.utcnow() - timedelta(days=1)
y, m, d = yesterday.year, yesterday.month, yesterday.day

# read country codes from CSV file and make API requests for each country
df_countries = pd.read_csv("API_Requests/countries_iso.csv", delimiter=';',encoding='latin-1')
for index, row in df_countries.iterrows():
    # extract country code and name from CSV row
    country_code=row["Alpha-2 code"]
    country_name=row["English short name"]
    # construct API URL for the specific country
    url = f"https://api.ebird.org/v2/data/obs/{country_code}/historic/{y}/{m}/{d}"
    # make GET request to eBird API    
    response = requests.get(url, headers=headers)
    if response.status_code !=200:
        print(f"Error fetching data for {country_name} ({country_code}): {response.status_code}")
        continue
    data = response.json()
    if not data:
        print(f"No data available for {country_name} ({country_code})")
        continue

    df=pd.json_normalize(response.json())
    df.insert(0, "countryCode", country_code)
    df.insert(1, "countryName", country_name)
    cols = ["countryCode","countryName","comName","sciName","obsDt","lat","lng","howMany","obsValid","obsReviewed","locationPrivate","subId"]
    for col in cols:
        if col not in df.columns:
            df[col] = pd.NA
    # keep only relevant columns : Country Code, Country Name, Common Name, Scientific Name, Observation Date, Latitude, Longitude, How Many, Obs Valid, Obs Reviewed, Location Private, Sub ID
    df = df[cols]
    # append the DataFrame for the current country to the overall DataFrame
    df_all=pd.concat([df_all,df], ignore_index=True)
    time.sleep(0.5) # To respect API rate limits

df_all = df_all.sort_values("obsDt")
pandas_gbq.to_gbq(df_all, "raw_ebird_daily.raw_ebird", project_id="daily-ebird", if_exists="append", credentials=credentials)
#print(response.text)