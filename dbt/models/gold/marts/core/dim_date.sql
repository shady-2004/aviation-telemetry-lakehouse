{{ config(materialized='table') }}

with spine as (
    -- Generates 1 row per day from 2020-01-01 through 2030-12-31
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2020-01-01' as date)",
        end_date="cast('2031-01-01' as date)"
    ) }}
),

base_dates as (
    select
        cast(date_day as date) as date_day
    from spine
)

select
    -- Surrogate Integer Key 
    cast(strftime(date_day, '%Y%m%d') as integer) as date_key,

    -- Base Calendar Date
    date_day,

    -- Year & Quarter Attributes
    extract(year from date_day) as year_number,
    extract(quarter from date_day) as quarter_number,
    concat('Q', extract(quarter from date_day), '-', extract(year from date_day)) as quarter_year_label,

    -- Month Attributes
    extract(month from date_day) as month_number,
    monthname(date_day) as month_name,
    strftime(date_day, '%b') as month_short_name,
    strftime(date_day, '%Y-%m') as year_month_label,

    -- Week & Day Attributes
    extract(week from date_day) as week_of_year,
    extract(day from date_day) as day_of_month,
    extract(dow from date_day) as day_of_week,
    dayname(date_day) as day_name,

    -- Boolean Flags for Dashboard Filtering
    case 
        when extract(dow from date_day) in (0, 6) then true 
        else false 
    end as is_weekend,
    
    case 
        when date_day = current_date then true 
        else false 
    end as is_today

from base_dates