with source as (
    select * from {{ ref('stg_postgres__airlines') }}
)

select
    -- Surrogate Key
    {{ dbt_utils.generate_surrogate_key([
        'coalesce(icao_code, iata_code, cast(airline_id as varchar))'
    ]) }} as airline_hk,

    -- Business Identifiers
    airline_id,
    icao_code,
    iata_code,
    callsign,

    -- Descriptive Attributes
    airline_name,
    alias_name,
    country_name,

    -- Operational Flag
    coalesce(is_active, false) as is_active

from source