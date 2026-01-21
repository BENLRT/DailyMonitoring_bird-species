with 

source as (

    select * from {{ source('raw_data', 'raw_iucn') }}

),

renamed as (

    select
        taxon_scientific_name AS scientific_name,
        year_published AS year,
        red_list_category_code

    from source

)

select * from renamed