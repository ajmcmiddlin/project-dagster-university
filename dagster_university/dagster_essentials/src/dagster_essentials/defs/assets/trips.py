import duckdb
import os
import requests
import dagster as dg
from dagster_essentials.defs.assets import constants
from dagster._utils.backoff import backoff

MONTH_TO_FETCH = "2023-03"

@dg.asset
def taxi_trips_file() -> None:
    """
        The raw parquet file with trips in it.
    """
    raw_trips = requests.get(f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{MONTH_TO_FETCH}.parquet")

    with open(constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(MONTH_TO_FETCH), "wb") as output_file:
        output_file.write(raw_trips.content)

@dg.asset
def taxi_zones_file() -> None:
    """
        The raw CSV file with NYC taxi zones in it.
    """
    raw_zones = requests.get("https://community-engineering-artifacts.s3.us-west-2.amazonaws.com/dagster-university/data/taxi_zones.csv")

    with open(constants.TAXI_ZONES_FILE_PATH, "wb") as output_file:
        output_file.write(raw_zones.content)

# src/dagster_essentials/defs/assets/trips.py
@dg.asset(
    deps=["taxi_trips_file"]
)
def taxi_trips() -> None:
    """
      The raw taxi trips dataset, loaded into a DuckDB database
    """
    _query = """
        create or replace table trips as (
          select
            VendorID as vendor_id,
            PULocationID as pickup_zone_id,
            DOLocationID as dropoff_zone_id,
            RatecodeID as rate_code_id,
            payment_type as payment_type,
            tpep_dropoff_datetime as dropoff_datetime,
            tpep_pickup_datetime as pickup_datetime,
            trip_distance as trip_distance,
            passenger_count as passenger_count,
            total_amount as total_amount
          from '{}'
        );
    """
    query = _query.format(constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(MONTH_TO_FETCH))

    conn = backoff(
        fn=duckdb.connect,
        retry_on=(RuntimeError, duckdb.IOException),
        kwargs={
            "database": os.getenv("DUCKDB_DATABASE"),
        },
        max_retries=10,
    )
    conn.execute(query)

@dg.asset(
    deps=["taxi_zones_file"]
)
def taxi_zones() -> None:
    """
        The raw taxi zones dataset, loaded into DuckDB
    """
    _query = """
        create or replace table zones as (
            select
                LocationID as zone_id,
                zone,
                borough,
                the_geom as geometry
            from '{}'
        );
    """
    query = _query.format(constants.TAXI_ZONES_FILE_PATH)

    conn = backoff(
        fn=duckdb.connect,
        retry_on=(RuntimeError, duckdb.IOException),
        kwargs={
            "database": os.getenv("DUCKDB_DATABASE"),
        },
        max_retries=10,
    )
    conn.execute(query)
