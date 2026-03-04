WITH daily_kpis AS (
    SELECT
        observation_date,
        ,COUNT(DISTINCT sub_id) AS total_observers
        ,SUM(individual_count) AS total_observations
        ,COUNT(DISTINCT scientific_name) AS distinct_species
    FROM {{ ref('intermediate_join_ebird_iucn') }}
    WHERE observation_date 
        BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 8 DAY)
            AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    GROUP BY observation_date
)

SELECT *
FROM daily_kpis