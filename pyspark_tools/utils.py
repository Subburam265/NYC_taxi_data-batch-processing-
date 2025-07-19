import requests
import pandas as pd
from data_cleaning import clean_nyc_data
from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.functions import date_format, avg, unix_timestamp
from pyspark.sql.functions import (
    col, when, month, hour, to_timestamp, 
    concat_ws, to_date, to_timestamp
)
from functools import reduce

def prepare_nyc_trip_data(paths: list) -> DataFrame:
    """
    Applies all NYC taxi trip data transformations in sequence:
    1. Unions all input parquet files into a single DataFrame
    2. Renames raw columns to standardized names
    3. Adds location-based zone, borough, and service zone columns
    4. Adds time-based features like trip duration, hour, day of week, and month
    5. Adds weather-related features using historical weather data
    6. Drops unneeded columns for a cleaner final table
    7. Adds engineered features for machine learning
    Returns:
        DataFrame: Transformed NYC taxi trip data ready for analysis or modeling
    """
    df = union_df(paths)
    df = clean_nyc_data(df)
    df = rename_nyc(df)
    df = add_location_labels(df)
    df = add_time_labels(df)
    df = add_weather_labels(df)
    df = drop_useless_columns(df)
    df = add_ml_features(df)
    df = clean_nyc_data(df)

    df.limit(100).write.mode("overwrite").parquet("data/output_data/sample_transformed_output.parquet")

    return df

def rename_col(df: DataFrame, new_col: dict) -> DataFrame:

    # Build a new column list: rename if in dictionary, else keep original
    renamed_columns = [
        F.col(old_col).alias(new_col.get(old_col, old_col))
        for old_col in df.columns
    ]

    # Return the DataFrame with selected (and possibly renamed) columns
    return df.select(renamed_columns)

def day_of_week_number(timestamp_col: str) -> F.Column:
    """
    Adds a numeric day-of-week column (1 = Monday, 7 = Sunday) to the DataFrame.
    Compatible with Spark < 3.0 (no 'u' pattern).
    Returns:
        Column expression for numeric weekday (1=Mon, ..., 7=Sun)
    """
    weekday_expr = F.when(F.date_format(timestamp_col, "E") == "Mon", 1) \
                    .when(F.date_format(timestamp_col, "E") == "Tue", 2) \
                    .when(F.date_format(timestamp_col, "E") == "Wed", 3) \
                    .when(F.date_format(timestamp_col, "E") == "Thu", 4) \
                    .when(F.date_format(timestamp_col, "E") == "Fri", 5) \
                    .when(F.date_format(timestamp_col, "E") == "Sat", 6) \
                    .when(F.date_format(timestamp_col, "E") == "Sun", 7) \
                    .otherwise(None)

    return weekday_expr

def fetch_weather_data(start_date: str, end_date: str, latitude: float = 40.7128, longitude: float = -74.0060) -> pd.DataFrame:
    """
    Fetches hourly NYC weather data (temperature, precipitation, wind speed) between given dates using Open-Meteo API.
    """
    url = (
        "https://archive-api.open-meteo.com/v1/archive"
        f"?latitude={latitude}&longitude={longitude}"
        f"&start_date={start_date}&end_date={end_date}"
        "&hourly=temperature_2m,precipitation,windspeed_10m"
        "&timezone=America%2FNew_York"
    )

    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    return pd.DataFrame({
        "time": data["hourly"]["time"],
        "temperature": data["hourly"]["temperature_2m"],
        "precipitation": data["hourly"]["precipitation"],
        "windspeed": data["hourly"]["windspeed_10m"],
    })

def rename_nyc(df: DataFrame):
    rename_map = {
    "VendorID": "driver_id",
    "tpep_pickup_datetime": "start_time",
    "tpep_dropoff_datetime": "end_time",
    "passenger_count": "passenger_count",
    "trip_distance": "distance_miles",
    "RatecodeID": "rate_code_id",
    "store_and_fwd_flag": "stored_forwarded",
    "PULocationID": "pickup_location",
    "DOLocationID": "dropoff_location",
    "fare_amount": "fare",
    "extra": "extra_fees",
    "mta_tax": "mta_tax",
    "tip_amount": "tip",
    "tolls_amount": "tolls",
    "improvement_surcharge": "improvement_fee",
    "total_amount": "total_paid",
    "congestion_surcharge": "congestion_fee",
    "Airport_fee": "airport_fee",
    "cbd_congestion_fee": "cbd_fee"
    }

    return rename_col(df,rename_map)

def union_df(path: list) -> DataFrame:
    spark = SparkSession.builder.getOrCreate()
    df_list = [spark.read.parquet(p) for p in path]
    df_all = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), df_list)
    df_all = df_all.filter(
    (F.col("tpep_pickup_datetime") >= F.lit("2025-01-01")) &
    (F.col("tpep_pickup_datetime") < F.lit("2025-04-01"))
    )
    return df_all

def add_location_labels(
    df: DataFrame,
    pickup_col: str = "pickup_location",
    dropoff_col: str = "dropoff_location"
) -> DataFrame:
    """
    Enrich DataFrame with borough, zone, and service_zone details for pickup & dropoff.
    Uses rename_col internally for consistency.
    """
    spark = df.sparkSession
    zones_df=spark.read.csv( "file:///home/subburam/NYC_project/data/enrichment_data/taxi_zone_lookup.csv",  header=True,  inferSchema=True).cache()
    zones_df.count()  # Force materialization of the cached DF

    pickup_renames = {
        "LocationID": pickup_col,
        "Borough": "pickup_borough",
        "Zone": "pickup_zone",
        "service_zone": "pickup_service_zone"
    }

    dropoff_renames = {
        "LocationID": dropoff_col,
        "Borough": "dropoff_borough",
        "Zone": "dropoff_zone",
        "service_zone": "dropoff_service_zone"
    }

    pickup = rename_col(zones_df, pickup_renames)
    dropoff = rename_col(zones_df, dropoff_renames)

    df = df.join(pickup, on=pickup_col, how="left") \
           .join(dropoff, on=dropoff_col, how="left")

    # Filter out rows with bad borough values
    bad_values = ["Unknown", "N/A", "EWR"]
    df = df.filter(
        ~col("pickup_borough").isin(bad_values) &
        ~col("dropoff_borough").isin(bad_values)
    )

    return df

def add_time_labels(df: DataFrame) -> DataFrame:
    """
    Adds useful time-based features:
    - trip_duration_min (in full minutes, floored)
    - day_of_week (e.g., Mon, Tue)
    - hour_of_day (0 to 23)
    """
    return df.withColumn(
        "trip_duration_min",
        (unix_timestamp("end_time") - unix_timestamp("start_time")) / 60
    ).withColumn(
        "hour_of_day",
        F.hour("start_time")
    ).withColumn(
        "day_of_week",
        day_of_week_number("start_time")
    ).withColumn(
        "date",
        F.to_date("start_time")
    ).withColumn(
        "month", F.month("start_time")
    ).withColumn(
        "speed_mph",
        when(col("trip_duration_min") > 0, col("distance_miles") / (col("trip_duration_min") / 60)).otherwise(F.lit(None))      )

def add_weather_labels(df: DataFrame) -> DataFrame:
    """
    Enrich NYC taxi trip data with hourly weather features: temperature, precipitation, windspeed.
    Uses Open-Meteo API to fetch historical weather data based on trip start time range.
    Returns:
        DataFrame: Spark DataFrame joined with weather data.
    """
    spark = df.sparkSession

    # Fetch weather data
    weather_df = spark.read.csv("file:///home/subburam/NYC_project/data/enrichment_data/weather_q1_2025.csv", header=True, inferSchema=True)

    # Preprocess timestamps for joining
    weather_df = weather_df.withColumn("time_ts", to_timestamp("time"))
    weather_df = weather_df.withColumn("date", to_date("time_ts")).withColumn("hour_of_day", hour("time_ts"))

    # Join on date and hour
    df = df.join(
        weather_df.select("hour_of_day", "date", "temperature", "precipitation", "windspeed"),
        on=["hour_of_day", "date"],
        how="left"
    )

    return df

def drop_useless_columns(df: DataFrame) -> DataFrame:
    df = df.withColumn(
    "other_fees",
    F.coalesce("extra_fees", F.lit(0)) +
    F.coalesce("mta_tax", F.lit(0)) +
    F.coalesce("tolls", F.lit(0)) +
    F.coalesce("improvement_fee", F.lit(0)) +
    F.coalesce("congestion_fee", F.lit(0)) +
    F.coalesce("airport_fee", F.lit(0)) +
    F.coalesce("cbd_fee", F.lit(0))
)
    columns_to_drop = ["pickup_location", "dropoff_location", "rate_code_id", "payment_type", "total_paid", "stored_forwarded", "extra_fees", "mta_tax", "tolls", "improvement_fee", "congestion_fee", "airport_fee", "cbd_fee"]
    return df.drop(*columns_to_drop)

def add_ml_features(df: DataFrame) -> DataFrame:
    """
    Adds basic engineered features based on existing columns.
    Includes: is_weekend, is_night, tip_percent, fare_per_mile, tipped,
              fare_per_min, is_raining, bad_weather, route
    """
    df = df.select(
        "*",  # Retain all original columns

        # 1. Weekend flag: 1 if Saturday or Sunday
        when(col("day_of_week").isin(6, 7), 1).otherwise(0).alias("is_weekend"),

        # 2. Night-time trip flag: 1 if between 10PM–6AM
        when((col("hour_of_day") >= 22) | (col("hour_of_day") < 6), 1).otherwise(0).alias("is_night"),

        # 3. Rush hour flag: 1 if between 7–10AM or 4–7PM
        when(col("hour_of_day").between(7, 10) | col("hour_of_day").between(16, 19), 1).otherwise(0).alias("is_rush_hour"),

        # 4. Tip percentage: tip as percent of fare
        when(col("fare") > 0, (col("tip") / col("fare")) * 100).otherwise(0.0).alias("tip_percent"),

        # 5. Fare per mile: normalized fare by trip distance
        when(col("distance_miles") > 0, col("fare") / col("distance_miles")).otherwise(0.0).alias("fare_per_mile"),

        # 6. Tipped flag: 1 if tip > 0
        when(col("tip") > 0, 1).otherwise(0).alias("tipped"),

        # 7. Fare per minute: fare normalized by trip duration
        when(col("trip_duration_min") > 0, col("fare") / col("trip_duration_min")).otherwise(0.0).alias("fare_per_min"),

        # 8. Rain indicator: 1 if precipitation > 0
        when(col("precipitation") > 0, 1).otherwise(0).alias("is_raining"),

        # 9. Bad weather: rain > 0.2 inches or windspeed > 20 km/h
        when((col("precipitation") > 0.2) | (col("windspeed") > 20), 1).otherwise(0).alias("bad_weather"),

        # 10. Route label: pickup_zone -> dropoff_zone string
        concat_ws("->", "pickup_zone", "dropoff_zone").alias("route")
    )
    return df

