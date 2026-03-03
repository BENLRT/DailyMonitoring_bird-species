# import necessary libraries
import requests
import pandas as pd 
import pandas_gbq
from google.oauth2 import service_account
import time
import os
import json
import base64
from datetime import datetime, timedelta, timezone

# define API key and base URL for eBird API
api_key = os.environ["EBIRD_APIKEY"]
headers = {"X-eBirdApiToken": api_key}

# Set up Google BigQuery credentials
credentials_b64 = os.environ["BQ_SERVICE_ACCOUNT"]
credentials_info = base64.b64decode(credentials_b64).decode("utf-8")
credentials_json = json.loads(credentials_info)
credentials = service_account.Credentials.from_service_account_info(credentials_json)

# initialize empty DataFrame to store all results
df_all=pd.DataFrame()

# calculate yesterday's date
yesterday = datetime.now(timezone.utc) - timedelta(days=1)
y, m, d = yesterday.year, yesterday.month, yesterday.day

# read country codes from CSV file and make API requests for each country
df_countries = pd.read_csv("API_Requests/countries.csv", delimiter=';',encoding='utf-8-sig')
# extract country code and name from CSV row for the United States
dic_us = json.load(open("API_Requests/dic_us.json", "r"))
# columns to keep in the final DataFrame
cols = ["countryCode","countryName","regionName","comName","sciName","obsDt","lat","lng","howMany","obsValid","obsReviewed","locationPrivate","subId"]
all_data = []
# iterate over each country in the CSV and make API requests
for index, row in df_countries.iterrows():
    # extract country code and name from CSV row
    country_code=row["ISO-alpha2 Code"]
    country_name=row["Country or Area"]
    region_name = row["Region Name"]
    # construct API URL for the specific country
    # US is a special case as we want to get data for each state separately
    if country_code == "US":
        for us_code, us_name in dic_us.items():
            data_us = None
            url = f"https://api.ebird.org/v2/data/obs/{us_code}/historic/{y}/{m}/{d}"
            for attempt in range(3):
                try:
                    response = requests.get(url, headers=headers)
                    if response.status_code ==200:
                        data_us = response.json()
                        break  
                    # exit loop if request is successful
                    elif response.status_code >= 500:
                        print(f"Server error {response.status_code} for {us_name} ({us_code}), retrying...")
                        time.sleep(5) # wait for 5 seconds before retrying
                    else:
                        print(f"Error {response.status_code} for {us_name} ({us_code}), not retrying.")
                        break  # exit loop for client errors
                except requests.exceptions.RequestException as e:
                    print(f"Request failed for {us_name} ({us_code}): {e}")
                    continue
            if not data_us:
                print(f"No data available for {us_name} ({us_code})")
                continue
            for obs in data_us:
                obs["countryCode"] = country_code
                obs["countryName"] = country_name
                obs["regionName"] = region_name
            all_data.extend(data_us)
            time.sleep(1) # To respect API rate limits

    else:
        # construct API URL for the specific country
        data = None
        url = f"https://api.ebird.org/v2/data/obs/{country_code}/historic/{y}/{m}/{d}"
        # make GET request to eBird API
        for attempt in range(3):
            try:
                response = requests.get(url, headers=headers)
                if response.status_code ==200:
                    data = response.json()
                    break  
                    # exit loop if request is successful
                elif response.status_code >= 500:
                    print(f"Server error {response.status_code} for {country_name} ({country_code}), retrying...")
                    time.sleep(5) # wait for 5 seconds before retrying
                else:
                    print(f"Error {response.status_code} for {country_name} ({country_code}), not retrying.")
                    break  # exit loop for client errors
            except requests.exceptions.RequestException as e:
                print(f"Request failed for {country_name} ({country_code}): {e}")
                continue

        if not data:
            print(f"No data available for {country_name} ({country_code})")
            continue
        for obs in data:
            obs["countryCode"] = country_code
            obs["countryName"] = country_name
            obs["regionName"] = region_name
        all_data.extend(data)
        time.sleep(1) # To respect API rate limits

if not all_data:
    print("No data collected for any region.")
else:
    df_all = pd.json_normalize(all_data)
    if df_all.empty:
        print("Empty dataframe after normalization.")
    else:
        for col in cols:
            if col not in df_all.columns:
                df_all[col] = pd.NA
        # keep only relevant columns : Country Code, Country Name, Common Name, Scientific Name, Observation Date, Latitude, Longitude, How Many, Obs Valid, Obs Reviewed, Location Private, Sub ID
        df_all = df_all[cols]
        df_all = df_all.sort_values("obsDt")
        pandas_gbq.to_gbq(df_all, "raw_ebird_daily.raw_ebird", project_id="daily-ebird", if_exists="append", credentials=credentials)