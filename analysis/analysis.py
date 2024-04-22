import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from google.cloud import bigquery

import os

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/felipe/gcp-service-accounts/nyc-citibike-419421-bd6994488451.json'

sns.set_style("darkgrid")

def format_number(num):
    if num > 1000000:
        if not num % 1000000:
            return f'{num // 1000000} M'
        return f'{round(num / 1000000, 1)} M'
    return f'{num // 1000} K'

@st.cache_data
def query_bigquery(limit=1000):
    client = bigquery.Client()

    project_id = "nyc-citibike-419421"
    dataset_id = "dbt_fmonteiro"
    table_id = "int_rides"

    # Specify the full table ID (including project and dataset)
    full_table_id = f"{project_id}.{dataset_id}.{table_id}"

    # Write a query to access the table
    query = f"select * from `{full_table_id}` order by rand() limit {limit}"

    # Make an API request
    query_job = client.query(query)

    # Wait for the query to finish
    results = query_job.to_dataframe()

    return results


# Page configuration
st.set_page_config(
    page_title="NYC CitiBike Rides",
    page_icon="🚴",
    layout="wide",
    initial_sidebar_state="expanded")

# Load data
df = query_bigquery()

df.dropna(inplace=True)

# TODO: Pass this on upstram to compute on BigQuery
df['year'] = pd.to_datetime(df['start_date']).dt.year

df['day_of_week'] = pd.Categorical(pd.to_datetime(df['start_date']).dt.day_name(),
                                   categories=["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"])

with st.sidebar:
    st.title('🚴 NYC CitiBike Dashboard')

    year_list = sorted(list(df['year'].unique()))
    selected_start_date = st.selectbox("Start date", year_list, index=len(year_list) - 1)
    df_selected_year = df[df["year"] == selected_start_date]

    year_list = sorted(list(df['year'].unique()))
    selected_end_date = st.selectbox("End date", year_list, index=len(year_list) - 1)
    # df_selected_year = df[df["year"] == selected_end_date]

    start_station_list = sorted(list(df['start_station_name'].unique()))
    selected_start_station = st.selectbox("Start station", start_station_list, index=len(start_station_list) - 1)

    end_station_list = sorted(list(df['end_station_name'].unique()))
    selected_end_station = st.selectbox("End station", end_station_list, index=len(end_station_list) - 1)


col = st.columns(2, gap='medium')

with col[0]:
    st.metric(label='Rides in total', value=len(df_selected_year))

    df['journey'] = df['start_station_name'] + ' > ' + df['end_station_name']
    df['same_station'] = df['start_station_name'] == df['end_station_name']

    st.pyplot(sns.displot(data=df, x='ride_duration'))

with col[1]:
    st.metric(label='Average ride duration (minutes)', value=round(df_selected_year['ride_duration'].mean()/60))
    st.pyplot(sns.catplot(data=df, y='day_of_week', kind='count'))



result = df.groupby(['journey', 'same_station']).agg(ride_count=('ride_duration', 'count'), average_ride_duration=('ride_duration', 'mean'))
result['average_ride_duration'] = round(result['average_ride_duration']/60, 2)
st.dataframe(result.sort_values(by='ride_count', ascending=False), use_container_width=True)