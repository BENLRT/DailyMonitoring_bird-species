WITH daily_obs AS (
    SELECT
        country_name
        ,SUM(individual_count) AS total_observations
    FROM {{ ref('intermediate_join_ebird_iucn') }}
    WHERE observation_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AND is_threatened_species = 1
    GROUP BY
        country_name
)

SELECT *
FROM daily_obs
ORDER BY total_observations
LIMIT 10