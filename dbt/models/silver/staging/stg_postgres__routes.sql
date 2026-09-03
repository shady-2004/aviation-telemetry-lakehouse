with source as (
    select * from {{ source('postgres', 'raw_routes') }}
),

cleaned as (
    select 
        -- Identifiers
        case 
            when trim(replace(airline, '"', '')) in ('', '\N', 'None', '-', '???') then null 
            else upper(trim(replace(airline, '"', '')))
        end as airline_code,

        case 
            when trim(replace(source_airport, '"', '')) in ('', '\N', 'None', '-', '???') then null 
            else upper(trim(replace(source_airport, '"', '')))
        end as source_airport_code,

        case 
            when trim(replace(destination_airport, '"', '')) in ('', '\N', 'None', '-', '???') then null 
            else upper(trim(replace(destination_airport, '"', '')))
        end as destination_airport_code,
            
        -- Route Attributes
        case 
            when trim(replace(equipment, '"', '')) in ('', '\N', 'None', '-', '???') then null 
            else upper(trim(replace(equipment, '"', '')))
        end as equipment_codes,
        
        coalesce(try_cast(trim(replace(stops::varchar, '"', '')) as integer), 0) as stop_count,

        -- Flags
        coalesce(upper(trim(replace(codeshare::varchar, '"', ''))) = 'Y', false) as is_codeshare

    from source
),

renamed as (
    select
        airline_code,
        source_airport_code,
        destination_airport_code,
        equipment_codes,
        stop_count,
        is_codeshare
    from cleaned

    where airline_code is not null
      and source_airport_code is not null
      and destination_airport_code is not null
      and source_airport_code != destination_airport_code
        
    qualify row_number() over (
        partition by airline_code, source_airport_code, destination_airport_code
        order by 
            case when stop_count = 0 then 1 else 2 end,
            case when not is_codeshare then 1 else 2 end,  -- Prefer the operating carrier over the codeshare
            case when equipment_codes is not null then 1 else 2 end
    ) = 1
)

select * from renamed