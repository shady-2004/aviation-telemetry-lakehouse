
with flight_pings as (
    select * from {{ ref('int_flights__enriched') }}
),

routes as (
    select 
        route_hk,
        airline_code,
        source_airport_code,
        destination_airport_code,
        route_distance_km
    from {{ ref('dim_routes') }}
),

airlines as (
    select 
        airline_hk,
        icao_code 
    from {{ ref('dim_airlines') }}
),

-- Aggregate flight activity per airline / date
daily_airline_metrics as (
    select
        cast(strftime(f.position_timestamp, '%Y%m%d') as integer) as date_key,
        f.partition_date,
        f.airline_icao_code,
        
        count(distinct f.callsign_code) as distinct_flights,
        count(distinct f.icao_address) as distinct_aircraft,
        count(f.flight_ping_hk) as total_pings,
        
        round(avg(case when f.flight_phase = 'cruising' then f.velocity_knots end), 1) as avg_cruise_speed,
        round(max(case when f.flight_phase = 'cruising' then f.velocity_knots end), 1) as max_cruise_speed,
        round(avg(case when f.flight_phase = 'cruising' then f.baro_altitude_feet end), 0) as avg_cruise_altitude,
        count(case when f.emergency_status != 'normal' then 1 end) as emergency_events

    from flight_pings f
    group by 1, 2, 3
)

select
    -- Primary Surrogate Key
    {{ dbt_utils.generate_surrogate_key([
        'm.date_key',
        'r.route_hk'
    ]) }} as daily_route_summary_hk,

    -- Foreign Keys
    m.date_key,
    r.route_hk,
    a.airline_hk,

    -- Degenerate Route Business Keys
    r.airline_code,
    r.source_airport_code,
    r.destination_airport_code,

    -- Additive & Summary Measures
    m.distinct_flights as total_flights_operated,
    m.distinct_aircraft as distinct_aircraft_count,
    m.total_pings as total_telemetry_pings,
    round(m.distinct_flights * coalesce(r.route_distance_km, 0), 1) as total_distance_flown_km,

    -- Operational Averages
    m.avg_cruise_speed as avg_cruise_speed_knots,
    m.max_cruise_speed as max_cruise_speed_knots,
    m.avg_cruise_altitude as avg_cruise_altitude_feet,
    m.emergency_events as emergency_events_count,

    -- Audit Date
    m.partition_date

from daily_airline_metrics m
inner join routes r
    on m.airline_icao_code = r.airline_code
left join airlines a
    on r.airline_code = a.icao_code