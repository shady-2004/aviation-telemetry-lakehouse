with source as (
    select * from {{ source('postgres', 'raw_routes') }}
),

cleaned as (
    select 
        -- Identifiers
        case 
            when trim(airline) in ('','\N','None','-') then null else upper(trim(airline))
            end as airline_code,
        case 
            when trim(source_airport) in ('','\N','None','-') then null else upper(trim(source_airport))
            end as source_airport_code,
        case 
            when trim(destination_airport) in ('','\N','None','-') then null else upper(trim(destination_airport))
            end as destination_airport_code,
            
        -- Route Attributes
        case 
            when trim(equipment) in ('','\N','None','-') then null else upper(trim(equipment))
            end as equipment_codes,
        
        stops::integer as stop_count,

        -- Flags
        case 
            when upper(trim(codeshare)) = 'Y' then true
            else false
        end as is_codeshare,

        -- Metadata
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
        and coalesce(airline_code, source_airport_code, destination_airport_code) is not null
        
    qualify row_number() over (
        partition by coalesce(airline_code, source_airport_code, destination_airport_code)
        order by 
            case when stop_count = 0 then 1 else 2 end,
            case when is_codeshare then 1 else 2 end,
            case when equipment_codes is not null then 1 else 2 end
    ) = 1
)

select * from renamed

