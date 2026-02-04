SELECT 
    e.sub_id,
    e.country_code,
    e.country_name,
    e.region_name,
    e.common_name,
    e.ebird_scientific_name,
    e.scientific_name,
    e.observation_date,
    e.observation_time,
    e.latitude,
    e.longitude,
    e.individual_count,
    e.is_validated,
    e.is_reviewed,
    e.is_location_private,
    i.global_status,
    if(e.region_name = "Europe", 
        CASE
            WHEN i.europe_status IS NOT NULL THEN i.europe_status
            WHEN i.mediterranean_status IS NOT NULL THEN i.mediterranean_status
            ELSE i.global_status
        END, 
        global_status
    ) AS regional_status,
FROM {{ ref( 'stg_raw_data__raw_ebird') }} AS e
LEFT JOIN {{ ref('intermediate_iucn_pivot') }} AS i
ON e.scientific_name = i.scientific_name