from dagster import Definitions, load_assets_from_modules

from .assets import rides
from .resources import database_resource

ride_assets = load_assets_from_modules([rides])

defs = Definitions(
    assets=[*ride_assets],
    resources={
        "database": database_resource
    },
)
