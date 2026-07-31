with source as (
    select * from {{ source('ourairports', 'raw_airports') }}
),

renamed as (
    select 
        -- Identifiers
        upper(trim(ident)) as airport_ident,
        nullif(trim(icao_code),'') as icao_code,
        nullif(trim(iata_code),'') as iata_code,
        nullif(trim(local_code),'') as local_code,
        nullif(trim(gps_code),'') as gps_code,

        -- Descriptive Attributes
        trim(name) as airport_name,
        trim(type) as airport_type,

        -- Spatial / Geography
        latitude_deg::double as latitude,
        longitude_deg::double as longitude,
        elevation_ft::integer as elevation_ft,
        nullif(trim(continent) , '') as continent , 
        nullif(trim(iso_country) , '') as iso_country ,
        nullif(trim(iso_region) , '') as iso_region ,
        nullif(trim(municipality) , '') as municipality,
        
        -- Flags
        scheduled_service::boolean as scheduled_service,
        
        from source 
        WHERE ident is not null 
        and trim(ident) != ''

        -- Deduplicate

        qualify row_number() over (
        partition by upper(trim(ident))
        order by 
            case when icao_code is not null then 1 else 2 end,
            case when iata_code is not null then 1 else 2 end
    ) = 1
)

select * from renamed