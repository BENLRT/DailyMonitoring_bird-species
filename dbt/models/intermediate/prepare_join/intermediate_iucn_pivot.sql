-- selecting each field which is the latest iucn status of a species
WITH base AS (
    SELECT
        scientific_name,
        scopes,
        -- rename red_list_category_code as status
        red_list_category_code AS status
    FROM {{ ref('stg_raw_data__raw_iucn') }}
    WHERE latest_status = TRUE
)
--- for each possible scope, create a column to pivot the iucn table (scopes are Europe, Mediterranean and Global for world status)
    SELECT
        scientific_name,
        MAX(CASE WHEN scopes = 'Global' THEN status END) AS global_status,
        MAX(CASE WHEN scopes = 'Europe' THEN status END) AS europe_status,
        MAX(CASE WHEN scopes = 'Mediterranean' THEN status END) AS mediterranean_status
    FROM base
    GROUP BY scientific_name
