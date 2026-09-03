with source as (
    select * from {{ source('ourairports', 'raw_airports') }}
),

cleaned as (
    select 
        -- Identifiers (strip quotes, trim, uppercase)
        upper(trim(replace(ident::varchar, '"', ''))) as airport_ident,
        
        -- Validate 4-character ICAO code
        case 
            when regexp_matches(upper(trim(replace(icao_code::varchar, '"', ''))), '^[A-Z0-9]{4}$')
            then upper(trim(replace(icao_code::varchar, '"', '')))
            else null
        end as icao_code,

        -- Validate 3-character IATA code
        case 
            when regexp_matches(upper(trim(replace(iata_code::varchar, '"', ''))), '^[A-Z0-9]{3}$')
            then upper(trim(replace(iata_code::varchar, '"', '')))
            else null
        end as iata_code,

        upper(nullif(trim(replace(local_code::varchar, '"', '')), '')) as local_code,
        upper(nullif(trim(replace(gps_code::varchar, '"', '')), '')) as gps_code,

        -- Descriptive Attributes
        nullif(trim(replace(name::varchar, '"', '')), '') as airport_name,
        lower(trim(replace(type::varchar, '"', ''))) as airport_type,

        -- Spatial / Geography (safe casts to prevent pipeline crashes)
        try_cast(latitude_deg as double) as latitude,
        try_cast(longitude_deg as double) as longitude,
        try_cast(elevation_ft as integer) as elevation_ft,
        
        nullif(trim(replace(continent::varchar, '"', '')), '') as continent, 
        nullif(trim(replace(iso_country::varchar, '"', '')), '') as iso_country, 
        nullif(trim(replace(iso_region::varchar, '"', '')), '') as iso_region, 
        nullif(trim(replace(municipality::varchar, '"', '')), '') as municipality,
        
        -- Flags
        coalesce(lower(trim(replace(scheduled_service::varchar, '"', ''))) in ('yes', '1', 'true'), false) as scheduled_service,
        coalesce(lower(trim(replace(type::varchar, '"', ''))) = 'closed', false) as is_closed

    from source 
),

renamed as (
    select 
        airport_ident,
        icao_code,
        iata_code,
        local_code,
        gps_code,
        airport_name,
        airport_type,
        latitude,
        longitude,
        elevation_ft,
        continent,
        iso_country,
        iso_region,
        municipality,
        scheduled_service,
        is_closed
    from cleaned
    
    -- Filter out records with missing primary identifiers or coordinates
    where airport_ident is not null 
      and airport_ident != ''
      and latitude between -90.0 and 90.0
      and longitude between -180.0 and 180.0

    -- Deduplicate identical airport identifiers
    qualify row_number() over (
        partition by airport_ident
        order by 
            case when icao_code is not null then 1 else 2 end,
            case when iata_code is not null then 1 else 2 end,
            case when not is_closed then 1 else 2 end
    ) = 1
)

select * from renamed