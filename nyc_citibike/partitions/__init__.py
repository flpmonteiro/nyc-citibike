from datetime import datetime
from dagster import MonthlyPartitionsDefinition, TimeWindowPartitionsDefinition
from ..assets import constants

start_date = constants.START_DATE
end_date = constants.END_DATE

historic_start_year = constants.HISTORIC_START_YEAR
historic_end_year = constants.HISTORIC_END_YEAR

monthly_partition = MonthlyPartitionsDefinition(
    start_date=start_date,
    end_date=end_date,
    fmt="%Y%m"
)

yearly_partition = TimeWindowPartitionsDefinition(
    start=historic_start_year,
    end=historic_end_year,
    cron_schedule="0 0 1 1 *",
    fmt="%Y"
)