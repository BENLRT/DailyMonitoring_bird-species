WITH global_status AS (
    SELECT
        scientific_name,
        scopes as region_name,
        red_list_category_code AS global_status
    FROM {{ ref('stg_raw_data__raw_iucn') }}
    WHERE scopes = 'Global'
      AND latest_status = true
),
regional_status AS (
    SELECT
        scientific_name,
        scopes AS region_name,
        red_list_category_code AS regional_status
    FROM {{ ref('stg_raw_data__raw_iucn') }}
    WHERE scopes != 'Global'
      AND latest_status = true
)
SELECT 
    rs.scientific_name,
    rs.region_name,
    gs.global_status,
    rs.regional_status
FROM regional_status AS rs
LEFT JOIN global_status AS gs
    ON rs.scientific_name = gs.scientific_name

UNION ALL

SELECT 
    scientific_name,
    scopes AS region_name,
    red_list_category_code AS global_status,
    "Global" AS regional_status
FROM {{ ref('stg_raw_data__raw_iucn') }}
    WHERE scopes = 'Global'
      AND latest_status = true
