# Monitoring-bird-species_daily
## Context
eBird is a citizen science platform managed by the Cornell Lab of Ornithology. By an app, people can save what bird they have observed.
This allow eBird to 

## Objective
Analyze daily bird observations and explore their relationship with the IUCN conservation status.

## Key Questions
- How many birds are observed ?
- What proportion of observed species are threatened according to the IUCN Red List?
- Which species are observed in the dataset?
- What is the distribution of observed species by conservation status?
- When are the birds being observed ?

## How to ? 
The goal is to unify eBird data and UICN Data : 
- Collect observation data using the eBird API
- Collect conservation status with the IUCN API
- Enrich the dataset with IUCN Red List data

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
    - Store the project code
- Bigquery : Store the Data to use
- DBT : Transform the data and create table for analysis
- Looker Studio : Visualisation of the data





