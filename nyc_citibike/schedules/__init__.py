from dagster import ScheduleDefinition
from ..jobs import rides_update_job

rides_update_schedule = ScheduleDefinition(
    job=rides_update_job,
    cron_schedule="0 0 5 * *", # every 5th of the month at midnight
)