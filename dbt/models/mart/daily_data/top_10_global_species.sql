WITH daily_obs AS (
    SELECT
        scientific_name,
        common_name,
        iucn_global_status,
        SUM(individual_count) AS total_observations
    FROM {{ ref('intermediate_join_ebird_iucn') }}
    WHERE observation_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    GROUP BY
        scientific_name,
        common_name,
        iucn_global_status
)

SELECT *
FROM daily_obs
ORDER BY total_observations DESC