with
    rides as (select * from {{ ref("int_rides") }}),

    station_to_station_counts as (
        select
            start_station_id,
            end_station_id,
            start_station_name,
            end_station_name,
            count(*) as count
        from rides
        group by start_station_id, end_station_id, start_station_name, end_station_name

    )

select *
from station_to_station_counts
order by count desc
