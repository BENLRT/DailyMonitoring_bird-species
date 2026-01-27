with 

source as (

    select * from {{ source('raw_data', 'raw_ebird') }}

),
parsed_data as (
    SELECT
        *,
        COALESCE(
            SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M:%S', obsdt),
            SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M', obsdt),
            SAFE.PARSE_DATETIME('%Y-%m-%d', obsdt)
        ) AS obs_datetime
    FROM source
    WHERE 
        countrycode IS NOT NULL 
        AND countryname IS NOT NULL 
        AND obsdt IS NOT NULL 
        AND lng IS NOT NULL
        AND subid IS NOT NULL
),
renamed as (

    select
        subid AS sub_id,
        countrycode AS country_code,
        countryname AS country_name,
        if(regionname IS NULL AND countrycode = "AQ", "Antartica",regionname) AS region_name,
        comname AS common_name,
        sciname AS scientific_name,
        DATE(obs_datetime) AS observation_date,
        TIME(obs_datetime) AS observation_time,
        lat AS latitude,
        lng AS longitude,
        CAST(if(howmany IS NULL, 1,howmany) AS INT) AS individual_count,
        obsvalid AS is_validated,
        obsreviewed AS is_reviewed,
        locationprivate AS is_location_private

    from parsed_data

)

select * from renamed