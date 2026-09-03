with source as (
    select * from {{ source('postgres', 'raw_airlines') }}
),

cleaned as (
    select
        -- Identifiers
        case 
            when regexp_matches(trim(replace(upper(icao::varchar), '"', '')), '^[A-Z]{3}$') 
            then trim(replace(upper(icao::varchar), '"', ''))
            else null 
        end as icao_code,
        
        case 
            when regexp_matches(trim(replace(upper(iata::varchar), '"', '')), '^[A-Z0-9]{2}$') 
            then trim(replace(upper(iata::varchar), '"', ''))
            else null 
        end as iata_code,

        case 
            when trim(replace(upper(callsign::varchar), '"', '')) in ('\N', 'NONE', 'NULL', '-', '', 'UNKNOWN', '?')
            then null
            else trim(replace(upper(callsign::varchar), '"', ''))
        end as callsign,

        case 
            when trim(replace(name, '"', '')) in ('\N', 'None', 'null', '-', '', 'Unknown', '???')
            then null
            else trim(replace(name, '"', ''))
        end as airline_name,

        case 
            when trim(replace(alias, '"', '')) in ('\N', 'None', 'null', '-', '', 'Unknown', '???')
            then null
            else trim(replace(alias, '"', ''))
        end as alias_name,

        case 
            when trim(replace(country, '"', '')) in ('\N', 'None', 'null', '-', '', 'Unknown', '???')
            then null
            else trim(replace(country, '"', ''))
        end as country_name,

        -- Active Flag
        coalesce(upper(trim(replace(active::varchar, '"', ''))) = 'Y', false) as is_active,
        
        -- Source ID
        airline_id

    from source 
),

renamed as (
    select
        icao_code,
        iata_code,
        callsign,
        airline_name,
        alias_name,
        country_name,
        is_active,
        airline_id
    from cleaned

    where airline_id != -1 
      and airline_name is not null
      and coalesce(icao_code, iata_code) is not null
    
    qualify row_number() over (
        partition by coalesce(icao_code, iata_code)
        order by 
            case when icao_code is not null then 1 else 2 end,
            case when is_active then 1 else 2 end,
            case when callsign is not null then 1 else 2 end,
            airline_id desc
    ) = 1
)

select * from renamed