from nyc_citibike.assets.rides import download_extract_historic_ride_data
from dagster import AssetSelection, define_asset_job
from ..partitions import monthly_partition, yearly_partition

historic_bike_rides = AssetSelection.keys("download_extract_historic_ride_data")

historic_rides_update_job = define_asset_job(
    name="rides_update_job",
    partitions_def=yearly_partition,
    selection=historic_bike_rides
)