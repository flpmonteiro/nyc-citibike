import zipfile
import os
import glob
from dagster_gcp.bigquery.resources import bigquery_resource
import requests
import pandas as pd
from io import BytesIO

from google.cloud import bigquery
from google.api_core.exceptions import NotFound

from dagster import asset, MaterializeResult, MetadataValue
from dagster_duckdb import DuckDBResource
from dagster_gcp import BigQueryResource

from . import constants
from ..partitions import monthly_partition, yearly_partition


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
    partitions_def=yearly_partition,
    group_name="raw_files",
)
def download_extract_historic_ride_data(context) -> None:
    """
    Download files of Citi Bike trip data.
    """
    year = context.partition_key

    url = constants.HISTORIC_DOWNLOAD_URL.format(year)
    raw_file_path = constants.RAW_FILE_PATH

    # Download the zip file
    print(f"Starting download from url {url}")
    response = requests.get(url)
    zip_content = BytesIO(response.content)

    # Use zipfile to extract CSV files
    with zipfile.ZipFile(zip_content) as zip_ref:
        # List all the file names in the zip
        for file_name in zip_ref.namelist():
            # Check if the file is a CSV
            if file_name.endswith(".csv"):
                # Extract the file to the specified directory
                zip_ref.extract(file_name, raw_file_path)


@asset(
    deps=["download_extract_historic_ride_data"],
    partitions_def=yearly_partition,
    group_name="duckdb",
)
def bike_rides_to_duckdb(context, database: DuckDBResource) -> None:
    """
    The bike rides data ingest into a DuckDB instance.
    """
    partition_date_str = context.partition_key
    year_month = partition_date_str

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
        from '{constants.RAW_FILE_PATH.format(year_month)}';
    """

    with database.get_connection() as conn:
        conn.execute(query)


@asset(
    deps=["download_extract_historic_ride_data"],
    group_name="bigquery",
)
def create_bigquery_table(bigquery_resource: BigQueryResource):
    dataset = "nyc_citibike_data"
    table_name = "rides_raw"
    project_id = bigquery_resource.project
    full_table_id = f"{project_id}.{dataset}.{table_name}"

    # Schema capable of handling current format and previous format
    schema = [bigquery.SchemaField(k, v) for k, v in constants.SCHEMA.items()]

    table = bigquery.Table(full_table_id, schema=schema)

    with bigquery_resource.get_client() as client:
        try:
            client.get_table(full_table_id)
        except NotFound:
            client.create_table(table)


@asset(
    deps=["download_extract_historic_ride_data"],
    partitions_def=yearly_partition,
)
def convert_csv_to_parquet(context) -> None:
    year = context.partition_key
    data_dir = constants.RAW_FILE_PATH
    pattern = os.path.join(data_dir, f"{year}-citibike-tripdata", "**", "*.csv")
    csv_files = glob.glob(pattern, recursive=True)

    for csv_file_path in csv_files:
        df = pd.read_csv(csv_file_path, dtype=str)
        df.to_parquet(os.path.splitext(csv_file_path)[0] + ".parquet", index=False)


@asset(
    deps=["create_bigquery_table", "convert_csv_to_parquet"],
    partitions_def=yearly_partition,
    group_name="bigquery",
)
def bike_rides_to_bigquery(context, bigquery_resource: BigQueryResource):
    dataset = "nyc_citibike_data"
    table_name = "rides_raw"
    main_table = f"{bigquery_resource.project}.{dataset}.{table_name}"
    staging_table = (
        f"{bigquery_resource.project}.{dataset}.{table_name}_staging_parquet"
    )

    year = context.asset_partition_key_for_output()
    data_dir = constants.RAW_FILE_PATH
    pattern = os.path.join(data_dir, f"{year}-citibike-tripdata", "**", "*.parquet")
    parquet_files = glob.glob(pattern, recursive=True)

    merge_query = f"""
            MERGE INTO `{main_table}` AS main
            USING `{staging_table}` AS staging
            ON main.ride_id = staging.ride_id
            WHEN NOT MATCHED BY TARGET THEN
                INSERT ({",".join(constants.SCHEMA.keys())})
                VALUES (
                    staging.ride_id,
                    staging.rideable_type,
                    staging.started_at,
                    staging.ended_at,
                    staging.start_station_name,
                    staging.start_station_id,
                    staging.end_station_name,
                    staging.end_station_id,
                    staging.start_lat,
                    staging.start_lng,
                    staging.end_lat,
                    staging.end_lng,
                    staging.member_casual
                    )
        """

    job_config = bigquery.LoadJobConfig(
        # schema=[bigquery.SchemaField(k, v) for k, v in constants.SCHEMA_NEW.items()],
        # skip_leading_rows=1,  # Skip header row in CSV files
        source_format=bigquery.SourceFormat.PARQUET,
    )

    with bigquery_resource.get_client() as client:
        for file_path in parquet_files:
            print(f"Begin loading data from {os.path.basename(file_path)}")
            with open(file_path, "rb") as f:
                # Load data into a staging table
                job = client.load_table_from_file(
                    file_obj=f, destination=staging_table,
                    job_config=job_config
                )
                job.result()
                print(
                    f"Done loading data from {os.path.basename(file_path)} into staging"
                )

                # Use SQL to merge staging data into the main table, avoiding duplicates
                # query_job = client.query(merge_query)
                # query_job.result()

                # Clean up the staging table after merge
                # client.delete_table(staging_table, not_found_ok=True)
