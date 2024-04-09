from dagster import EnvVar
from dagster_duckdb import DuckDBResource
from dagster_gcp import BigQueryResource

duckdb = DuckDBResource(
    database=EnvVar("DUCKDB_DATABASE")
    )

bigquery_resource = BigQueryResource(
    project=EnvVar("GCP_PROJECT")
)