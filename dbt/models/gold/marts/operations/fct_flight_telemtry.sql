{{ config(materialized='table') }}

with flights as (
    select * from {{ ref('int_flights__enriched') }}
),

airlines as (
    select 
        airline_hk, 
        icao_code 
    from {{ ref('dim_airlines') }}
)

select
    -- Primary Surrogate Key (Atomic Ping Grain)
    f.flight_ping_hk,

    -- Foreign Dimension Keys
    cast(strftime(f.position_timestamp, '%Y%m%d') as integer) as date_key,
    a.airline_hk,

    -- Degenerate Dimensions & Identifiers
    f.icao_address,
    f.callsign_code,
    f.airline_icao_code,
    f.squawk_code,
    f.flight_phase,
    f.emergency_status,
    f.position_source_id,

    -- Spatial Coordinates & Kinematic Measures
    f.latitude,
    f.longitude,
    f.baro_altitude_feet,
    f.geo_altitude_feet,
    f.velocity_knots,
    f.vertical_rate_fpm,
    f.heading_degrees,

    -- Operational Status Flags
    f.is_on_ground,
    f.is_spi,

    -- Timestamps & Ingestion Partition
    f.position_timestamp,
    f.last_seen_timestamp,
    f.partition_date

from flights f
left join airlines a
    on f.airline_icao_code = a.icao_code