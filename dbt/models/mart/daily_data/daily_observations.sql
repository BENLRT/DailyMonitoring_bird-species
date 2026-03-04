WITH daily_obs AS (
    SELECT
        observation_date
        ,COUNT(DISTINCT sub_id) AS total_observers
        ,SUM(individual_count) AS total_observations
        ,COUNT(DISTINCT scientific_name) AS total_distinct_species
        ,ROUND(SAFE_DIVIDE(SUM(individual_count),COUNT(DISTINCT sub_id)),0) individuals_per_checklist
        ,SUM(is_threatened_species) AS total_threatened_species_observed
    FROM {{ ref('intermediate_join_ebird_iucn') }}
    WHERE observation_date 
        BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 8 DAY)
            AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    GROUP BY observation_date
)

SELECT *
FROM daily_obs