import zipfile
from io import BytesIO
from dagster._core.definitions import metadata
from dagster._core.definitions import partition
import requests
import pandas as pd
from dagster import asset, MaterializeResult, MetadataValue
from dagster_duckdb import DuckDBResource

from . import constants


def download_and_extract(url: str, destination_path: str) -> bool:
    try:
        response = requests.get(url)
        response.raise_for_status()  # This will raise an exception for 4xx/5xx errors

        # Open a file-like BytesIO object, obtained from response, as a zip file
        with zipfile.ZipFile(BytesIO(response.content)) as zip_file:
            # Get the name of the first file in the zip archive
            file_name = zip_file.namelist()[0]

            # Open the extracted file and save to disk
            with zip_file.open(file_name) as csv_file, open(
                destination_path, "wb"
            ) as output_file:
                output_file.write(csv_file.read())
        return True
    except requests.RequestException as e:
        print(f"Request error: {e}")
    except zipfile.BadZipFile as e:
        print(f"Zip file error: {e}")
    return False


@asset(
    group_name="raw_files",
)
def bike_rides_file() -> MaterializeResult:
    """
    Download files of Citi Bike trip data.
    """
    year_month = "202309"

    url = constants.DOWNLOAD_URL.format(year_month)
    raw_file_path = constants.BIKE_RIDES_FILE_PATH.format(year_month)

    # Attempt to download and save file to disk.
    # If successful, load as DataFrame to return some metadata
    if download_and_extract(url, raw_file_path):
        print(f"File saved successfully to {raw_file_path}")
        df = pd.read_csv(raw_file_path)
        return MaterializeResult(
            metadata={
                "Number of records": len(df),
                "Preview": MetadataValue.md(df.head().to_markdown()),
            }
        )
    else:
        print(f"Failed to download or extract the file")
        return MaterializeResult(metadata={"Error": "Download or extraction failed"})


@asset(
    deps=["bike_rides_file"],
    group_name="ingested",
)
def bike_rides(database: DuckDBResource) -> None:
    """
    The bike rides data ingest into a DuckDB instance.
    """
    year_month = "202309"

    query = f"""
        create table if not exists rides (
            ride_id varchar,
            rideable_type varchar,
            started_at timestamp,
            ended_at timestamp,
            start_station_name varchar,
            start_station_id varchar,
            end_station_name varchar,
            end_station_id varchar,
            start_latitude float,
            start_longitude float,
            end_latitude float,
            end_longitude float,
            member_or_casual_ride varchar,
            partition_date varchar
        );

        delete from rides where partition_date = '{year_month}';

        insert into rides
            select
                ride_id,
                rideable_type,
                started_at,
                ended_at,
                start_station_name,
                start_station_id,
                end_station_name,
                end_station_id,
                start_lat,
                start_lng,
                end_lat,
                end_lng,
                member_casual,
                '{year_month}' as partition_date
        from '{constants.BIKE_RIDES_FILE_PATH.format(year_month)}';
    """

    with database.get_connection() as conn:
        conn.execute(query)
