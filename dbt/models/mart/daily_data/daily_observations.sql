--- Aggregate daily observations by country and IUCN status
WITH daily_obs AS (
    SELECT
        observation_date
        ,country_code
        ,country_name
        ,iucn_global_status
        ,COUNT(DISTINCT sub_id) AS total_observers
        ,SUM(individual_count) AS total_observations
        ,COUNT(DISTINCT scientific_name) AS total_distinct_species
        ,ROUND(SAFE_DIVIDE(SUM(individual_count),COUNT(DISTINCT sub_id)),0) individuals_per_checklist
        ,SUM(is_threatened_species) AS total_threatened_species_observed
    FROM {{ ref('intermediate_join_ebird_iucn') }}
    --- Keep only the 2 last days
    WHERE observation_date 
        BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
            AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    GROUP BY 
        observation_date 
        ,iucn_global_status
        ,country_code
        ,country_name
)
--- Calculate the previous day's metrics for comparison
, lag_last_day_obs AS (
    SELECT
    observation_date
    ,country_code
    ,country_name
    ,iucn_global_status
    ,total_observers
    ,total_observations
    ,total_distinct_species
    ,individuals_per_checklist
    ,total_threatened_species_observed
    --- LAG to get total observations from the previous day
    ,LAG(total_observations) 
        OVER (PARTITION BY country_code, country_name, iucn_global_status ORDER BY observation_date ASC) AS last_day_total_observations
    --- LAG to get individuals per checklists from the previous day
    ,LAG(individuals_per_checklist) 
        OVER (PARTITION BY country_code, country_name, iucn_global_status ORDER BY observation_date ASC) AS last_day_total_individual_per_checklist
    FROM daily_obs
)
SELECT *
FROM lag_last_day_obs