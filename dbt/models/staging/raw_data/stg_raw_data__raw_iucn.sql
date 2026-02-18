with 

source as (

    select * from {{ source('raw_data', 'raw_iucn') }}

),

renamed as (

    select
        taxon_scientific_name AS scientific_name
        ,year_published AS year
        --- Normalizing former IUCN categories 
        ,CASE
            WHEN red_list_category_code = 'LR/lc' THEN 'LC'
            WHEN red_list_category_code = 'LR/nt' THEN 'NT'
            WHEN red_list_category_code = 'LR/cd' THEN 'NT'
            WHEN red_list_category_code = 'nt' THEN 'NT'
            WHEN red_list_category_code = 'NR' THEN 'NE'
            WHEN red_list_category_code = 'T' THEN 'NT'
            ELSE red_list_category_code
        END AS red_list_category_code
        ,scopes
        ,latest as latest_status

    from source
    WHERE latest = True

)

select * from renamed