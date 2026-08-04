with source as (
    select * from {{ source('opensky', 'raw_flights') }}
),

-- Flatten the root JSON 
flattened as (
    select 
        date as partition_date,
        to_timestamp(time::bigint) as fetch_timestamp,
        unnest(states) as state
    from source

),

renamed as (
    select 
        -- Identifiers
        lower(trim(state[1]::varchar)) as icao_address , 
        nullif(trim(state[2]::varchar) , '') as callsign_code ,
        nullif(trim(state[3]::varchar),'') as origin_country , 

        -- Derivied icao code
        case
            when length(nullif(trim(state[2]::varchar),'')) >= 3 
            then upper(substring(trim(state[2]::varchar),1,3))
            else null
        end as icao_code,

        -- Timestamps
        to_timestamp(state[4]::bigint) as position_timestamp , 
        to_timestamp(state[5]::bigint) as last_seen_timestamp,

        -- Telemetry & Coordinates
        state[6]::double as latitude,
        state[7]::double as longitude,
        state[8]::double as baro_altitude_meters,
        state[14]::double as geo_altitude_meters, 
        state[10]::double as velocity_mps,
        state[11]::double as true_track_degrees,
        state[12]::double as vertical_rate_mps, 

        -- Flags 

        coalesce(state[9]::boolean, false) as is_on_ground,
        coalesce(state[16]::boolean, false) as is_spi,
        nullif(trim(state[15]::varchar), '') as squawk_code,
        state[17]::integer as position_source_id,

        partition_date

        from flattened

        --Quality Filters
        where state[1] is not null                                 -- Must have ICAO 
        and (state[7]::double between -90 and 90 or state[7] is null)  -- Validate latitude range
        and (state[6]::double between -180 and 180 or state[6] is null) -- Validate longitude range

        -- Deduplicate identical state vectors sent in the same batch
        qualify row_number() over (
        partition by lower(trim(state[1]::varchar)), to_timestamp(state[4]::bigint)
        order by fetch_timestamp desc
        ) = 1

)

select * from renamed