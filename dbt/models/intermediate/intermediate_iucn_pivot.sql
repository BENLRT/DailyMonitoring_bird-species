WITH base AS (
    SELECT
        scientific_name,
        scopes,
        red_list_category_code AS status
    FROM {{ ref('stg_raw_data__raw_iucn') }}
    WHERE latest_status = TRUE
)
    SELECT
        scientific_name,
        MAX(CASE WHEN scopes = 'Global' THEN status END) AS global_status,
        MAX(CASE WHEN scopes = 'Europe' THEN status END) AS europe_status,
        MAX(CASE WHEN scopes = 'Mediterranean' THEN status END) AS mediterranean_status
    FROM base
    GROUP BY scientific_name
