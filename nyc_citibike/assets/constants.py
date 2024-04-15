RAW_FILE_PATH = "data/raw"

RAW_FILE_NAME = "bike_rides_{}.csv"

# HISTORIC_DOWNLOAD_URL = "https://s3.amazonaws.com/tripdata/{}-citibike-tripdata.zip"
HISTORIC_DOWNLOAD_URL = "http://127.0.0.1:8000/{}-citibike-tripdata.zip"

CURRENT_DOWNLOAD_URL = "https://s3.amazonaws.com/tripdata/{}-citibike-tripdata.csv.zip"

START_DATE = "202401"
END_DATE = "202403"

HISTORIC_START_YEAR = "2020"
HISTORIC_END_YEAR = "2024"

SCHEMA = {
    "ride_id": "STRING",
    "rideable_type": "STRING",
    "start_time": "STRING",
    "stop_time": "STRING",
    "start_station_name": "STRING",
    "start_station_id": "STRING",
    "end_station_name": "STRING",
    "end_station_id": "STRING",
    "start_station_latitude": "STRING",
    "start_station_longitude": "STRING",
    "end_station_latitude": "STRING",
    "end_station_longitude": "STRING",
    "user_type": "STRING",
    "trip_duration": "STRING",
    "bike_id": "STRING",
    "birth_year": "STRING",
    "gender": "STRING",
}

SCHEMA_OLD = {
    "trip_duration": "STRING",
    "start_time": "STRING",
    "stop_time": "STRING",
    "start_station_id": "STRING",
    "start_station_name": "STRING",
    "start_station_latitude": "STRING",
    "start_station_longitude": "STRING",
    "end_station_id": "STRING",
    "end_station_name": "STRING",
    "end_station_latitude": "STRING",
    "end_station_longitude": "STRING",
    "bike_id": "STRING",
    "user_type": "STRING",
    "birth_year": "STRING",
    "gender": "STRING",
}


SCHEMA_NEW = {
    "ride_id": "STRING",
    "rideable_type": "STRING",
    "start_time": "STRING",
    "stop_time": "STRING",
    "start_station_name": "STRING",
    "start_station_id": "STRING",
    "end_station_name": "STRING",
    "end_station_id": "STRING",
    "start_station_latitude": "STRING",
    "start_station_longitude": "STRING",
    "end_station_latitude": "STRING",
    "end_station_longitude": "STRING",
    "user_type": "STRING",
}
