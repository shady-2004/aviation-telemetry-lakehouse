with flights as (
    select * from {{ ref('stg_opensky__flights') }}
),

airlines as (
    select * from {{ ref('stg_postgres__airlines') }}
),

enriched as (
    select
        -- Primary Surrogate Key (Atomic Grain: Aircraft + Ping Timestamp)
        {{ dbt_utils.generate_surrogate_key([
            'f.icao_address',
            'f.position_timestamp'
        ]) }} as flight_ping_hk,

        --  Aircraft & Call Identifiers
        f.icao_address,
        f.callsign_code,
        f.icao_code as airline_icao_code,
        f.origin_country as transponder_country,

        --  Airline Reference Join
        a.airline_name,
        a.country_name as airline_country,
        coalesce(a.is_active, false) as is_airline_active,

        --  Spatial Position & Timestamps
        f.position_timestamp,
        f.last_seen_timestamp,
        f.latitude,
        f.longitude,
        f.partition_date,

        --  Standardized Kinematics & Aviation Unit Conversions
        f.baro_altitude_meters,
        round(f.baro_altitude_meters * 3.28084, 1) as baro_altitude_feet,
        
        f.geo_altitude_meters,
        round(f.geo_altitude_meters * 3.28084, 1) as geo_altitude_feet,

        f.velocity_mps,
        round(f.velocity_mps * 1.94384, 1) as velocity_knots,

        f.vertical_rate_mps,
        round(f.vertical_rate_mps * 196.85, 1) as vertical_rate_fpm, -- Feet per minute

        f.true_track_degrees as heading_degrees,

        --  Operational Status & Flags
        f.is_on_ground,
        f.is_spi,
        f.squawk_code,
        f.position_source_id,

        --  Derived Flight Phase Classification
        case
            when f.is_on_ground then 'ground'
            when f.vertical_rate_mps > 2.5 then 'climbing'
            when f.vertical_rate_mps < -2.5 then 'descending'
            when f.baro_altitude_meters >= 4500 then 'cruising'
            else 'level_flight'
        end as flight_phase,

        --  Emergency Squawk Tagging
        case
            when f.squawk_code = '7500' then 'unlawful_interference'
            when f.squawk_code = '7600' then 'radio_failure'
            when f.squawk_code = '7700' then 'general_emergency'
            else 'normal'
        end as emergency_status

    from flights f
    left join airlines a
        on f.icao_code = a.icao_code
)

select * from enriched