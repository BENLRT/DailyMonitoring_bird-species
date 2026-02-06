WITH iucn AS (
    SELECT
        *
    FROM {{ ref('intermediate_iucn_pivot') }}
),
ebird AS (
    SELECT
        *
    FROM {{ ref( 'intermediate_join_matrix_gbiftaxonomy_ebird') }}
),
joined AS (
    SELECT 
        e.sub_id,
        e.country_code,
        e.country_name,
        e.region_name,
        e.`order`,
        e.family,
        e.genus,
        e.common_name,
        e.scientific_name,
        e.observation_date,
        e.observation_time,
        e.latitude,
        e.longitude,
        e.individual_count,
        e.is_validated,
        e.is_reviewed,
        e.is_location_private,
        -- keep global status
        i.global_status AS iucn_global_status,
        -- keep region status. AS the mediterranean region is not an iso region. We keep mediterranean for Europe
        if(e.region_name = "Europe", 
            CASE
                WHEN i.europe_status IS NOT NULL THEN i.europe_status
                WHEN i.mediterranean_status IS NOT NULL THEN i.mediterranean_status
                ELSE i.global_status
            END, 
            i.global_status
        ) AS iucn_regional_status
    FROM ebird AS e
    LEFT JOIN iucn AS i
    ON e.scientific_name = i.scientific_name
)
SELECT
    sub_id,
    country_code,
    country_name,
    region_name,
    `order`,
    family,
    genus,
    common_name,
    scientific_name,
    observation_date,
    observation_time,
    latitude,
    longitude,
    individual_count,
    is_validated,
    is_reviewed,
    is_location_private,
    IF(iucn_global_status IS NULL, "Missing data", iucn_global_status) AS iucn_global_status,
    IF(iucn_regional_status IS NULL, "Missing data", iucn_regional_status) AS iucn_regional_status
FROM joined