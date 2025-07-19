from functools import reduce
from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.functions import col, unix_timestamp

def clean_nyc_data(df: DataFrame) -> DataFrame:
    
    iqr_filter_cols = {
    "fare_amount": {"min": 2.5},
    "trip_distance": {"min": 0.1},
    "extra": {"min":0},
    "tip_amount": {"min": 0},
    "mta_tax": {"min": 0},
    "tolls_amount": {"min": 0},
    "improvement_surcharge": {"min": 0},
    "total_amount": {"min": 0},
    "congestion_surcharge": {"min": 0},
    "Airport_fee": {"min": 0},
    "cbd_congestion_fee": {"min": 0},
    "trip_duration_min": {"min": 1},
    "speed_mph": {"min": 1},
    "fare_per_mile": {"min": 0},
    "fare_per_min": {"min": 0}
    }

    critical_cols = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "trip_distance",
    "fare_amount",
    "total_amount",
    "passenger_count",
    "PULocationID",
    "DOLocationID"
    ]

    non_critical_cols = [
    "tip_amount",
    "extra",
    "mta_tax",
    "tolls_amount",
    "improvement_surcharge",
    "congestion_surcharge",
    "Airport_fee",
    "cbd_congestion_fee",
    "temperature",
    "precipitation",
    "windspeed"
    ]
    # Null data handling
    null_garb = null_garbage(df, critical_cols)
    df = drop_null(df, critical_cols)
    df = fill_null(df, non_critical_cols)
    
    # Outlier data handling
    iqr_garb = iqr_garbage(df, iqr_filter_cols)
    df = iqr_filter(df, iqr_filter_cols)
   
    if "passenger_count" in df.columns:
        df = df.filter(F.col("passenger_count") > 0)

    # Saving the garbage data for restoration or deletion
    null_garb.write.mode("overwrite").parquet("/NYC_taxi_data_batch_processing/data/output_data/garbage_data/null_data")
    iqr_garb.write.mode("overwrite").parquet("/NYC_taxi_data_batch_processing/data/output_data/garbage_data/skewed_data")

    return df

def iqr_filter(df: DataFrame, col_dict: dict, multiplier: float = 1.5) -> DataFrame:
    # Filters rows based on IQR and optional min/max constraints per column

    # Get all quantiles in one batch operation
    valid_columns = [col for col in col_dict.keys() if col in df.columns]
    if not valid_columns:
        return df

    quantile_results = df.approxQuantile(valid_columns, [0.25, 0.75], 0.01)

    conditions = []
    for i, column in enumerate(valid_columns):
        quantiles = quantile_results[i]  # estimate Q1 and Q3
        if None in quantiles or len(quantiles) < 2:
            continue  # skip if quantiles can't be computed
        q1, q3 = quantiles
        iqr = q3 - q1  # compute interquartile range
        lower_limit = q1 - multiplier * iqr  # lower bound
        upper_limit = q3 + multiplier * iqr  # upper bound
        cond = (F.col(column) >= lower_limit) & (F.col(column) <= upper_limit)  # filter within IQR

        constrain = col_dict[column]
        if "min" in constrain:
            cond = cond & (F.col(column) >= constrain["min"])  # apply min constraint
        if "max" in constrain:
            cond = cond & (F.col(column) <= constrain["max"])  # apply max constraint
        conditions.append(cond)  # store condition
    return df.filter(reduce(lambda a, b: a & b, conditions)) if conditions else df  # apply all conditions with AND

def iqr_garbage(df: DataFrame, col_dict: dict, multiplier: float = 1.5) -> DataFrame:
    """
    Inverts the IQR filter to capture bad/outlier rows using batched quantile calculations
    """
    # Get all valid columns and batch calculate quantiles
    valid_columns = [col for col in col_dict.keys() if col in df.columns]
    if not valid_columns:
        return df.limit(0)  # Return empty DataFrame with same schema
    
    # Batch quantile calculation - much more efficient
    quantile_results = df.approxQuantile(valid_columns, [0.25, 0.75], 0.01)
    
    conditions = []
    for i, column in enumerate(valid_columns):
        quantiles = quantile_results[i]
        if None in quantiles or len(quantiles) < 2:
            continue
            
        q1, q3 = quantiles
        iqr = q3 - q1
        
        # Handle edge case where IQR is 0
        if iqr == 0:
            cond = F.col(column).isNull()  # Start with null condition
        else:
            lower_limit = q1 - multiplier * iqr
            upper_limit = q3 + multiplier * iqr
            cond = (F.col(column) < lower_limit) | (F.col(column) > upper_limit)
        
        # Apply min/max constraints
        constrain = col_dict[column]
        if "min" in constrain:
            cond = cond | (F.col(column) < constrain["min"])
        if "max" in constrain:
            cond = cond | (F.col(column) > constrain["max"])
        
        conditions.append(cond)
    
    # Return filtered DataFrame or empty if no conditions
    return df.filter(reduce(lambda a, b: a | b, conditions)) if conditions else df.limit(0)

def drop_null(df: DataFrame, columns: list) -> DataFrame:
    # Keep only the columns that actually exist in the DataFrame
    valid_columns = [c for c in columns if c in df.columns]

    # If no valid columns, return same DataFrame
    if not valid_columns:
        return df

    # Drops rows with nulls in specified columns
    return df.dropna(how="any", subset=valid_columns)

def null_garbage(df: DataFrame, columns: list) -> DataFrame:
    # Keep only the columns that actually exist in the DataFrame
    valid_columns = [c for c in columns if c in df.columns]

    # If no valid columns, return empty DataFrame with same schema
    if not valid_columns:
        return df.limit(0)

    # Return rows where at least one of the specified columns is null
    return df.filter(reduce(lambda a, b: a | b, [F.col(c).isNull() for c in valid_columns]))

def fill_null(df: DataFrame, columns: list, val: float = 0.0) -> DataFrame:
    # Keep only the columns that actually exist in the DataFrame
    valid_columns = [c for c in columns if c in df.columns]

    # If no valid columns, return same DataFrame
    if not valid_columns:
        return df
    # Fills nulls in specified columns with a default value
    return df.fillna(val, subset=valid_columns)

