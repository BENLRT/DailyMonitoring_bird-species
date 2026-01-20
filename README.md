# Monitoring-bird-species_daily
This project will allow us to save daily observation of bird species and to observe how many common or rare species are observed per day. 

## How to ? 
The goal is to unify eBird data and UICN Data, so we can have the status of the species and the observation. 


## Technologies 
- Python :
    - Librairies : 
        - requests
        - os
        - pandas
        - pandas_gbq
        - json
        - time
        - google.oauth2
    - Extract Data from eBird API and UICN API
    - Keep only relevant columns
- Github : 
    - Launch pipeline to extract daily
    - Store the python code
- Bigquery : Store the Data to use
- DBT : Transform the data and create table for analysis
- Looker Studio, Python : Visualisation of the data





