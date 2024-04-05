from setuptools import find_packages, setup

setup(
    name="nyc_citibike",
    packages=find_packages(exclude=["nyc_citibike_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "dagster-duckdb",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
