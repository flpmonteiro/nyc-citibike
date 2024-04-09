from setuptools import find_packages, setup

setup(
    name="nyc_citibike",
    packages=find_packages(exclude=["nyc_citibike_tests"]),
    install_requires=[
        "dagster==1.6.*",
        "dagster-cloud",
        "dagster-duckdb",
        "dagster-gcp",
        "dagster-gcp-pandas",
        "pandas",
        "plotly",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
