with

    source as (select * from {{ source("nyc_citibike", "rides_raw") }}),

    renamed as (

        select
            ride_id,
            rideable_type,
            {{ clean_time("start_time") }} as start_time,
            {{ clean_time("stop_time") }} as stop_time,
            start_station_name,
            start_station_id,
            end_station_name,
            end_station_id,
            cast(start_station_latitude as numeric) as start_station_latitude,
            cast(start_station_longitude as numeric) as start_station_longitude,
            cast(end_station_latitude as numeric) as end_station_latitude,
            cast(end_station_longitude as numeric) as end_station_longitude,
            user_type,
            cast(trip_duration as int64) as ride_duration,
            cast(bike_id as int64) as bike_id,
            {{ clean_birth_year("birth_year") }} as birth_year,
            case
                when gender = '0'
                then 'Unkown'
                when gender = '1'
                then 'Male'
                when gender = '2'
                then 'Female'
                else null
            end as gender

        from source

    )

select *
from
    renamed

-- {% if var("is_test_run", default=true) %} limit 100 {% endif %}