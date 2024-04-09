from dagster import Definitions, load_assets_from_modules

from .assets import rides
from .resources import duckdb, bigquery_resource
from .jobs import rides_update_job
from .schedules import rides_update_schedule

ride_assets = load_assets_from_modules([rides])

all_jobs = [rides_update_job]
all_schedules = [rides_update_schedule]

defs = Definitions(
    assets=[*ride_assets],
    resources={
        "database": duckdb,
        "bigquery_resource": bigquery_resource,
    },
    jobs=all_jobs,
    schedules=all_schedules,
)
