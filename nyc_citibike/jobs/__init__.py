from dagster import AssetSelection, define_asset_job
from ..partitions import monthly_partition

rides_update_job = define_asset_job(
    name="rides_update_job",
    partitions_def=monthly_partition,
    selection=AssetSelection.all()
)