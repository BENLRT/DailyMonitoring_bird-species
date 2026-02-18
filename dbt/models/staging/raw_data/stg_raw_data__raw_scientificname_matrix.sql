with 

source as (

    select * from {{ source('raw_data', 'raw_scientificname_matrix') }}

),

renamed as (

    select
        english_name
        ,ebird_scientific_name
        ,scientific_name
        ,family
        ,`order`

    from source

)

select * from renamed