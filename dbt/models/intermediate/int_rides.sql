with
    rides_raw as (select * from {{ ref("stg_nyc_citibike__rides_raw") }}),

    rides as (
        select
            ride_id,
            rideable_type,
            start_time,
            stop_time,
            start_station_name,
            start_station_id,
            end_station_name,
            end_station_id,
            start_station_latitude,
            start_station_longitude,
            end_station_latitude,
            end_station_longitude,
            user_type,
            case
                when ride_duration is null
                then timestamp_diff(stop_time, start_time, second)
                else ride_duration
            end as ride_duration,
            bike_id,
            birth_year,
            gender

        from rides_raw
    )

select *
from
    rides

    -- {% if var("is_test_run", default=true) %} limit 100 {% endif %}
    
