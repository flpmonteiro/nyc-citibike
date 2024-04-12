RAW_FILE_PATH = "data/raw"

RAW_FILE_NAME = "bike_rides_{}.csv"

# HISTORIC_DOWNLOAD_URL = "https://s3.amazonaws.com/tripdata/{}-citibike-tripdata.zip"
HISTORIC_DOWNLOAD_URL = 'http://127.0.0.1:8000/{}-citibike-tripdata.zip'

CURRENT_DOWNLOAD_URL = "https://s3.amazonaws.com/tripdata/{}-citibike-tripdata.csv.zip"

START_DATE = "202401"
END_DATE = "202403"

HISTORIC_START_YEAR = "2020"
HISTORIC_END_YEAR = "2024"

SCHEMA = {"ride_id": "STRING",
          "rideable_type": "STRING",
          "start_time": "TIMESTAMP",
          "stop_time": "TIMESTAMP",
          "start_station_name": "STRING",
          "start_station_id": "STRING",
          "end_station_name": "STRING",
          "end_station_id": "STRING",
          "start_station_latitude": "NUMERIC",
          "start_station_longitude": "NUMERIC",
          "end_station_latitude": "NUMERIC",
          "end_station_longitude": "NUMERIC",
          "user_type": "STRING",
          "trip_duration": "NUMERIC",
          "bike_id": "INTEGER",
          "gender": "STRING",
          "birth_year": "STRING"
          }

SCHEMA_OLD = {"ride_id": "STRING",
              "rideable_type": "STRING",
              "started_at": "TIMESTAMP",
              "ended_at": "TIMESTAMP",
              "start_station_name": "STRING",
              "start_station_id": "STRING",
              "end_station_name": "STRING",
              "end_station_id": "STRING",
              "start_lat": "NUMERIC",
              "start_lng": "NUMERIC",
              "end_lat": "NUMERIC",
              "end_lng": "NUMERIC",
              "member_casual": "STRING"
              }


SCHEMA_NEW = {
    "ride_id": "STRING",
    "rideable_type": "STRING",
    "started_at": "STRING",
    "ended_at": "STRING",
    "start_station_name": "STRING",
    "start_station_id": "STRING",
    "end_station_name": "STRING",
    "end_station_id": "STRING",
    "start_lat": "STRING",
    "start_lng": "STRING",
    "end_lat": "STRING",
    "end_lng": "STRING",
    "member_casual": "STRING",
}