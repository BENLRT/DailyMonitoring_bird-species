with 

source as (

    select * from {{ source('raw_data', 'raw_gbif_taxonomy') }}

),

renamed as (

    select
        `order` 
        ,family 
        ,genus 
        ,species as scientific_name

    from source
    --- select species and validated taxonomic status and where the scientifc Name is not written with a x (Ex: name x name)
    WHERE taxonRank = "SPECIES" 
        AND taxonomicStatus = "ACCEPTED" 
        AND species NOT LIKE '% x %' 
    --- remove fossil
        AND family IS NOT NULL
        AND `order` IS NOT NULL
)

select * from renamed