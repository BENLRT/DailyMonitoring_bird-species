
WITH ebird_observations AS (
    SELECT 
        sub_id
        , country_code
        , country_name
        , region_name
        , `order`
        , family
        , genus
        , common_name
        , scientific_name
        , iucn_global_status
        , iucn_regional_status
        , observation_date
        , time_of_day
        , individual_count
        , SUM(individual_count)
            OVER (PARTITION BY observation_date) AS total_birds_observed
    FROM {{ ref('intermediate_join_ebird_iucn') }}
    WHERE observation_date 
        BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 8 DAY) 
            AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
)
, metrics AS (
    SELECT
        country_code
        , country_name
        , region_name
        , `order`
        , family
        , genus
        , common_name
        , scientific_name
        , iucn_global_status
        , iucn_regional_status
        , observation_date
        , time_of_day
        , SUM(individual_count) AS total_individuals
        , ROUND(SAFE_DIVIDE(SUM(individual_count),COUNT(DISTINCT sub_id)),2) individuals_per_checklist
        , MAX(total_birds_observed) AS total_birds_observed

    FROM ebird_observations
    GROUP BY country_code, 
        country_name, 
        region_name, 
        `order`, 
        family, 
        genus, 
        common_name,
        scientific_name, 
        iucn_global_status,
        iucn_regional_status, 
        observation_date, 
        time_of_day
)
SELECT 
    *
FROM metrics