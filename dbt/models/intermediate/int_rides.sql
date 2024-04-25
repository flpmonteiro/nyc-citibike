{{
    config(
        materialized='table'
    )
}}

with
    rides_raw as (select * from {{ ref("stg_nyc_citibike__rides_raw") }}),

    rides as (
        select
            cast(start_time as date) as start_date,
            cast(stop_time as date) as stop_date,
            cast(start_time as time) as start_time,
            cast(stop_time as time) as stop_time,
            start_station_name,
            end_station_name,
            case
                when ride_duration is null
                then timestamp_diff(stop_time, start_time, second)
                else ride_duration
            end as ride_duration,

        from rides_raw
    )

select *
from
    rides

    -- {% if var("is_test_run", default=true) %} limit 100 {% endif %}
    
