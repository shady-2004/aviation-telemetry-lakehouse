with airports as (
    select * from {{ref('stg_ourairports__airports')}}
)

select 
    -- Surrogate Key
    {{dbt_utils.generate_surrogate_key(['airport_ident'])}} as airport_hk,

    -- Business Identifiers
    airport_ident,
    icao_code,
    iata_code,
    local_code,
    gps_code,

    -- Descriptive Attributes
    airport_name,
    airport_type,

    -- Spatial / Geography
    latitude,
    longitude,
    elevation_ft,
    continent,
    iso_country,
    iso_region,
    municipality,

    -- Flags
    scheduled_service

from airports