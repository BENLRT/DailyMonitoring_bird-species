SELECT 
    sub_id,
    country_code,
    country_name,
    region_name,
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
    red_list_category_code as global_status,
    red_list_category_code as regional_status,
    i.global_status,
    i.regional_status
FROM {{ ref( 'stg_raw_data__raw_ebird') }} AS e
LEFT JOIN {{ ref('stg_raw_data__raw_iucn') }} AS i
ON e.scientific_name = i.scientific_name