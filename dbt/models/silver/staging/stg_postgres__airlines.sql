with source as (
    select * from {{ source('postgres', 'raw_airlines') }}
),

cleaned as (
    select
        -- Identifiers
        case 
            when trim(upper(icao::varchar)) ~ '^[A-Z0-9]{3}$' 
            then trim(upper(icao::varchar))
            else null 
        end as icao_code,
        
        case 
            when trim(upper(iata::varchar)) ~ '^[A-Z0-9]{2}$' 
            then trim(upper(iata::varchar))
            else null 
        end as iata_code,
        upper(nullif(nullif(nullif(nullif(trim(callsign),'\N'),'None'),'-'),'')) as callsign,

        -- Descriptive Attributes
        trim(name) as airline_name,
        nullif(nullif(nullif(nullif(trim(alias),'\N'),'None'),'-'),'') as alias_name,
        nullif(nullif(nullif(nullif(trim(country),'\N'),'None'),'-'),'') as country_name,

        -- Flags
        case 
            when upper(trim(active)) = 'Y' then true 
            else false
        end as is_active,
        
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
        and airline_name != 'Unknown'
        and airline_name != ''
        and coalesce(icao_code, iata_code) is not null
    
    qualify row_number() over (
        partition by coalesce(icao_code, iata_code)
        order by 
            case when icao_code is not null then 1 else 2 end,
            case when is_active then 1 else 2 end,
            case when callsign is not null then 1 else 2 end
    ) = 1
)

select * from renamed

