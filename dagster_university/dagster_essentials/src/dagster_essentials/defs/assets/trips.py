import requests
import dagster as dg
from dagster_duckdb import DuckDBResource
from dagster_essentials.defs.assets import constants
from dagster._utils.backoff import backoff
from dagster_essentials.defs.partitions import monthly_partition

def get_partition_month(context: dg.AssetExecutionContext) -> str:
    """
        Gets the partition key from the context and drops the last 3
        characters. The last three characters are the "-DD" part of
        the date, but the NYC data uses "YYYY-MM" as its date format.
    """
    return context.partition_key[:-3]

@dg.asset(
    partitions_def=monthly_partition
)
def taxi_trips_file(context: dg.AssetExecutionContext) -> None:
    """
        The raw parquet file with trips in it.
    """
    month_to_fetch = get_partition_month(context)
    raw_trips = requests.get(f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month_to_fetch}.parquet")

    with open(constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch), "wb") as output_file:
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
    deps=["taxi_trips_file"],
    partitions_def=monthly_partition
)
def taxi_trips(context: dg.AssetExecutionContext, database: DuckDBResource) -> None:
    """
      The raw taxi trips dataset, loaded into a DuckDB database
    """
    partition_month = get_partition_month(context)
    _query = """
        create table if not exists trips (
            vendor_id integer, pickup_zone_id integer, dropoff_zone_id integer,
            rate_code_id double, payment_type integer, dropoff_datetime timestamp,
            pickup_datetime timestamp, trip_distance double, passenger_count double,
            total_amount double, partition_date varchar
        );

        delete from trips
        where partition_date = '{partition_month}';

        insert into trips (
            select VendorID, PULocationID, DOLocationID, RatecodeID, payment_type,
                   tpep_dropoff_datetime, tpep_pickup_datetime, trip_distance,
                   passenger_count, total_amount, '{partition_month}' as partition_date
            from '{trips_file}'
        );
        """

    query = _query.format_map({
        "trips_file": constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(partition_month),
        "partition_month": partition_month
    })

    with database.get_connection() as conn:
        conn.execute(query)

@dg.asset(
    deps=["taxi_zones_file"]
)
def taxi_zones(database: DuckDBResource) -> None:
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

    with database.get_connection() as conn:
        conn.execute(query)
