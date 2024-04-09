from dagster import MonthlyPartitionsDefinition
from ..assets import constants

fmt = constants.DATE_FORMAT
start_date = constants.START_DATE
end_date = constants.END_DATE

monthly_partition = MonthlyPartitionsDefinition(
    start_date=start_date,
    end_date=end_date,
    fmt=fmt
)