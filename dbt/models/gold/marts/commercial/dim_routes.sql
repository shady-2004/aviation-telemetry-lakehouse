with routes as (
    select * from {{ref('int_routes__enriched')}}
)

select 
	-- Primary surrogate key
	route_hk,

	-- Buisness Identifiers
	airline_code, 
    source_airport_code,
    destination_airport_code,

    -- Origin Airport 
    origin_airport_name,
    origin_country,
    origin_latitude,
    origin_longitude,
    origin_elevation_ft,

    -- Destiantion Airport
    destination_airport_name,
    destination_country,
    destination_latitude,
    destination_longitude,
    destination_elevation_ft,

    -- Route Characteristics
    stop_count,
    is_codeshare,
    equipment_codes,
    route_distance_km

from routes