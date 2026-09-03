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

cleaned_state as (
    select
        state,
        partition_date,
        fetch_timestamp,
        nullif(trim(replace(state[2]::varchar, '"', '')), '') as callsign_clean
    from flattened
),

renamed as (
    select 
        -- Identifiers
        lower(trim(replace(state[1]::varchar, '"', ''))) as icao_address, 
        callsign_clean as callsign_code,
        nullif(trim(replace(state[3]::varchar, '"', '')), '') as origin_country, 

        -- Derived icao code
        case
            when regexp_matches(callsign_clean, '^[A-Z]{3}[0-9]') 
            then upper(substring(callsign_clean, 1, 3))
            else null
        end as icao_code,

        -- Timestamps
        to_timestamp(state[4]::bigint) as position_timestamp, 
        to_timestamp(state[5]::bigint) as last_seen_timestamp,

        -- Telemetry & Coordinates (state[7] is Lat, state[6] is Lon)
        state[7]::double as latitude,
        state[6]::double as longitude,
        state[8]::double as baro_altitude_meters,
        state[14]::double as geo_altitude_meters, 
        state[10]::double as velocity_mps,
        state[11]::double as true_track_degrees,
        state[12]::double as vertical_rate_mps, 

        -- Flags 
        coalesce(state[9]::boolean, false) as is_on_ground,
        coalesce(state[16]::boolean, false) as is_spi,
        nullif(trim(replace(state[15]::varchar, '"', '')), '') as squawk_code,
        state[17]::integer as position_source_id,

        partition_date

    from cleaned_state

    -- Quality Filters: Strictly enforce non-null spatial and temporal attributes
    where state[1] is not null                                      -- Must have ICAO address
      and state[4] is not null                                      -- Must have event timestamp
      and state[7] is not null                                      -- Must have latitude
      and state[6] is not null                                      -- Must have longitude
      and state[7]::double between -90.0 and 90.0                   -- Valid Latitude bounds
      and state[6]::double between -180.0 and 180.0                 -- Valid Longitude bounds

    -- Deduplicate identical state vectors sent in the same batch
    qualify row_number() over (
        partition by lower(trim(replace(state[1]::varchar, '"', ''))), to_timestamp(state[4]::bigint)
        order by fetch_timestamp desc
    ) = 1
)

select * from renamed