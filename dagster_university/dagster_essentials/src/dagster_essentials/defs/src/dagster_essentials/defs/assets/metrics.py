import dagster as dg

import datetime

import matplotlib.pyplot as plt
import geopandas as gpd
import pandas as pd

import duckdb
import os

from dagster_essentials.defs.assets import constants


def get_duck_conn() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(os.getenv("DUCKDB_DATABASE"))

@dg.asset(
    deps=["taxi_trips", "taxi_zones"]
)
def manhattan_stats() -> None:
    """
        geojson file for trip counts by zone in Manhattan
    """
    query = """
        select
            zones.zone,
            zones.borough,
            zones.geometry,
            count(1) as num_trips
        from trips
        left join zones on trips.pickup_zone_id = zones.zone_id
        where borough = 'Manhattan' and geometry is not null
        group by zone, borough, geometry
    """

    conn = get_duck_conn()
    trips_by_zone = conn.execute(query).fetch_df()

    trips_by_zone["geometry"] = gpd.GeoSeries.from_wkt(trips_by_zone["geometry"])
    trips_by_zone = gpd.GeoDataFrame(trips_by_zone)

    with open(constants.MANHATTAN_STATS_FILE_PATH, 'w') as output_file:
        output_file.write(trips_by_zone.to_json())

@dg.asset(
    deps=["manhattan_stats"],
)
def manhattan_map() -> None:
    """
        Map of trip counts by zone in Manhattan
    """
    trips_by_zone = gpd.read_file(constants.MANHATTAN_STATS_FILE_PATH)

    fig, ax = plt.subplots(figsize=(10, 10))
    trips_by_zone.plot(column="num_trips", cmap="plasma", legend=True, ax=ax, edgecolor="black")
    ax.set_title("Number of Trips per Taxi Zone in Manhattan")

    ax.set_xlim([-74.05, -73.90])  # Adjust longitude range
    ax.set_ylim([40.70, 40.82])  # Adjust latitude range
    
    # Save the image
    plt.savefig(constants.MANHATTAN_MAP_FILE_PATH, format="png", bbox_inches="tight")
    plt.close(fig)

@dg.asset(
    deps = ["taxi_trips"]
)
def trips_by_week() -> None:
    """
        CSV of trip data aggregated by week
    """

    def get_week_of_data(sunday: datetime.date) -> pd.DataFrame:
        """
            Given the date of a Sunday, retrieve a summary of the trips for the
            week starting on that Sunday.
        """

        week_of_data_query = '''
            SELECT
                DATE(pickup_datetime - INTERVAL (dayofweek(pickup_datetime)) DAY) as period,
                COUNT(*) as num_trips,
                SUM(passenger_count) as passenger_count,
                SUM(total_amount) as total_amount,
                SUM(trip_distance) as trip_distance
            FROM trips
            GROUP BY period
            HAVING period = '{}'
        '''
        week_of_data_query_formatted = week_of_data_query.format(sunday.isoformat())

        conn = get_duck_conn()
        week_of_data = conn.execute(week_of_data_query_formatted).fetch_df()

        return week_of_data

    sundays_query = """
        SELECT distinct(DATE(pickup_datetime - INTERVAL (dayofweek(pickup_datetime)) DAY)) as sunday
        FROM trips;
    """

    conn = get_duck_conn()
    sunday_rows = conn.execute(sundays_query).fetchall()
    sundays = [r[0] for r in sunday_rows]

    df = pd.DataFrame()

    for sunday in sundays:
        week_of_data = get_week_of_data(sunday)
        df = pd.concat([df, week_of_data])

    df["num_trips"] = df["num_trips"].astype(int)
    df["passenger_count"] = df["passenger_count"].astype(int)
    df["total_amount"] = df["total_amount"].round(2).astype("float")
    df["trip_distance"] = df["trip_distance"].round(2).astype("float")
    df = df.sort_values(by="period")
    df.to_csv(constants.TRIPS_BY_WEEK_FILE_PATH, index=False)
