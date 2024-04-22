import random
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from google.cloud import bigquery

import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
    "/home/felipe/gcp-service-accounts/nyc-citibike-419421-bd6994488451.json"
)

sns.set_style("darkgrid")


def format_number(num):
    if num > 1000000:
        if not num % 1000000:
            return f"{num // 1000000} M"
        return f"{round(num / 1000000, 1)} M"
    return f"{num // 1000} K"


@st.cache_data
def query_bigquery(
    start_date: str = "2013-06-08",
    stop_date: str = "2023-12-21",
    start_station=None,
    end_station=None,
    limit=10000,
):
    client = bigquery.Client()

    project_id = "nyc-citibike-419421"
    dataset_id = "dbt_fmonteiro"
    table_id = "int_rides"

    # Specify the full table ID (including project and dataset)
    full_table_id = f"{project_id}.{dataset_id}.{table_id}"

    start_station_filter = f"and start_station_name = '{start_station}'" if start_station else ''
    end_station_filter = f"and end_station_name = '{end_station}'" if end_station else ''
    add_limit = f"limit {limit}" if limit else ''

    # Write a query to access the table
    query = f"""
    select *,
    extract(year from start_date) as year,
    format_date('%a', start_date) as day_name,
    from `{full_table_id}`
    where start_date >= '{start_date}'
    and stop_date <= '{stop_date}'
    {start_station_filter}
    {end_station_filter}
    order by rand()
    {add_limit}
    """
    print(query)

    # Make an API request
    query_job = client.query(query)

    # Wait for the query to finish
    results = query_job.to_dataframe()

    return results


@st.cache_data
def query_min_max_dates():
    client = bigquery.Client()

    project_id = "nyc-citibike-419421"
    dataset_id = "dbt_fmonteiro"
    table_id = "int_rides"
    full_table_id = f"{project_id}.{dataset_id}.{table_id}"

    query = f"""
        select
            min(start_date) as min_date,
            max(start_date) as max_date
        from `{full_table_id}`
    """

    query_job = client.query(query)
    results = query_job.to_dataframe()
    return results


@st.cache_data
def query_station_names():
    client = bigquery.Client()

    project_id = "nyc-citibike-419421"
    dataset_id = "dbt_fmonteiro"
    table_id = "int_rides"
    full_table_id = f"{project_id}.{dataset_id}.{table_id}"

    query = f"""
        select
            distinct start_station_name as station_name
        from `{full_table_id}`
        where start_station_name is not null
        order by station_name
    """

    query_job = client.query(query)
    results = query_job.to_dataframe()
    return results

# Page configuration
st.set_page_config(
    page_title="NYC CitiBike Rides",
    page_icon="🚴",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Load data
min_max_dates = query_min_max_dates()
min_date = min_max_dates['min_date'].values[0]
max_date = min_max_dates['max_date'].values[0]
station_names = query_station_names()

with st.sidebar:
    st.title("🚴 NYC CitiBike Dashboard")

    selected_start_date = st.date_input(
        label="Start date",
        value=min_date,
        min_value=min_date,
        max_value=max_date,
    )

    selected_stop_date = st.date_input(
        label="Stop date",
        value=max_date,
        min_value=min_date,
        max_value=max_date,
    )

    selected_start_station = st.selectbox(
        label="Start station",
        options=station_names,
        index=None,
    )

    selected_end_station = st.selectbox(
        label="End station",
        options=station_names,
        index=None,
    )

    limit = st.number_input(
        label="Limit for query",
        min_value=0,
        value=10000,
    )


# Load data
df = query_bigquery(
    start_date=selected_start_date,
    stop_date=selected_stop_date,
    start_station=selected_start_station,
    end_station=selected_end_station,
    limit=limit,
)

col = st.columns(2, gap='medium')

with col[0]:
    st.metric(label='Rides in total', value=len(df))
    st.pyplot(sns.displot(data=df, x='ride_duration'))

with col[1]:
    st.metric(label='Average ride duration (minutes)', value=round(df['ride_duration'].mean()/60))
    st.pyplot(sns.catplot(data=df, y='day_name', kind='count'))


df['journey'] = df['start_station_name'] + ' > ' + df['end_station_name']
df['same_station'] = df['start_station_name'] == df['end_station_name']
result = df.groupby(['journey', 'same_station']).agg(ride_count=('ride_duration', 'count'), average_ride_duration=('ride_duration', 'mean'))
result['average_ride_duration'] = round(result['average_ride_duration']/60, 2)
st.write("Most popular routes")
st.dataframe(result.sort_values(by='ride_count', ascending=False), use_container_width=True)
