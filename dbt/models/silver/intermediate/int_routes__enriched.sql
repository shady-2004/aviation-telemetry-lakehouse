with routes as (
    select * from {{ ref('stg_postgres__routes') }}
),

airports as (
    select * from {{ ref('stg_ourairports__airports') }}
),

--  Build a strict 1:1 lookup mapping for each airport code
airport_lookup as (
    select
        coalesce(iata_code, airport_ident) as lookup_code,
        airport_name,
        airport_type,
        iso_country,
        latitude,
        longitude,
        elevation_ft
    from airports
    where coalesce(iata_code, airport_ident) is not null
    -- Prioritize operational and large airports in case of duplicate code collisions
    qualify row_number() over (
        partition by coalesce(iata_code, airport_ident)
        order by 
            case airport_type
                when 'large_airport' then 1
                when 'medium_airport' then 2
                when 'small_airport' then 3
                else 4
            end,
            scheduled_service desc
    ) = 1
),

-- 
joined as (
    select
        -- Deterministic Surrogate Key
        {{ dbt_utils.generate_surrogate_key([
            'r.airline_code',
            'r.source_airport_code',
            'r.destination_airport_code'
        ]) }} as route_hk,

        -- Route Identifiers
        r.airline_code,
        r.source_airport_code,
        r.destination_airport_code,
        r.equipment_codes,
        r.stop_count,
        r.is_codeshare,

        -- Origin Airport Metadata
        orig.airport_name as origin_airport_name,
        orig.airport_type as origin_airport_type,
        orig.iso_country as origin_country,
        orig.latitude as origin_latitude,
        orig.longitude as origin_longitude,
        orig.elevation_ft as origin_elevation_ft,

        -- Destination Airport Metadata
        dest.airport_name as destination_airport_name,
        dest.airport_type as destination_airport_type,
        dest.iso_country as destination_country,
        dest.latitude as destination_latitude,
        dest.longitude as destination_longitude,
        dest.elevation_ft as destination_elevation_ft,

        -- Great-Circle Distance (Safe from NULLs)
        case 
            when orig.latitude is not null and dest.latitude is not null
            then round(
                2 * 6371.0 * asin(
                    sqrt(
                        power(sin(radians(dest.latitude - orig.latitude) / 2), 2) +
                        cos(radians(orig.latitude)) * cos(radians(dest.latitude)) *
                        power(sin(radians(dest.longitude - orig.longitude) / 2), 2)
                    )
                ), 2
            )
            else null
        end as route_distance_km

    from routes r
    left join airport_lookup orig
        on r.source_airport_code = orig.lookup_code
    left join airport_lookup dest
        on r.destination_airport_code = dest.lookup_code
)

select * from joined