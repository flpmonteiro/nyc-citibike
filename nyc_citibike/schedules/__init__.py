from dagster import ScheduleDefinition
from ..jobs import historic_rides_update_job

historic_rides_update_schedule = ScheduleDefinition(
    job=historic_rides_update_job,
    cron_schedule="0 0 5 1 *", # yearly, at midnight of January 5th
)