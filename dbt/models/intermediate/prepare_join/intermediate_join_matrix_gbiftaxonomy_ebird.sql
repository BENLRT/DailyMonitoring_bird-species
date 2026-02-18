WITH ebird AS (
    SELECT 
        sub_id
        , country_code
        , country_name
        , region_name
        , common_name
        , ebird_scientific_name
        , observation_date
        , observation_time
        , latitude
        , longitude
        , individual_count
        , is_validated
        , is_reviewed
        , is_location_private
    FROM {{ ref('stg_raw_data__raw_ebird') }}
),
sc_name_matrix AS (
    SELECT 
        ebird_scientific_name
        , scientific_name
        , order
        , family
    FROM {{ ref('stg_raw_data__raw_scientificname_matrix') }}

),
gbif_taxonomy AS (
    SELECT 
        scientific_name
        ,order
        ,family
        ,genus
    FROM {{ ref('stg_raw_data__raw_gbif_taxonomy') }}
), 
normalized AS (
    SELECT
        e.sub_id
        ,e.country_code
        ,e.country_name
        ,e.region_name
        ,e.common_name
        ,scm.`order`
        ,scm.family
        --- if the corresponding iucn scientific name has not been found, keep the ebird scientific name, else keep icun one. (The species can be called differently)
        ,IF(
            scm.scientific_name= "Not Exist"
            ,e.ebird_scientific_name
            ,scm.scientific_name
        ) AS scientific_name
        ,e.observation_date
        ,e.observation_time
        ,e.latitude
        ,e.longitude
        ,e.individual_count
        ,e.is_validated
        ,e.is_reviewed
        ,e.is_location_private

    FROM ebird AS e
    LEFT JOIN sc_name_matrix AS scm
    ON e.ebird_scientific_name = scm.ebird_scientific_name
),
all_joined AS (
    SELECT
        n.sub_id
        ,n.country_code
        ,n.country_name
        ,n.region_name
        ,COALESCE(g.`order`,n.`order`,"Missing data") AS `order`
        ,COALESCE(g.family,n.family,"Missing data") AS family
        ,IF(
            g.genus IS NULL 
            ,SPLIT(n.scientific_name, ' ')[OFFSET(0)]
            ,g.genus
        ) AS genus
        ,n.common_name
        ,n.scientific_name
        ,n.observation_date
        ,n.observation_time
        ,n.latitude
        ,n.longitude
        ,n.individual_count
        ,n.is_validated
        ,n.is_reviewed
        ,n.is_location_private

    FROM normalized AS n
    LEFT JOIN gbif_taxonomy AS g
        ON n.scientific_name = g.scientific_name
)
SELECT 
    *
FROM all_joined
