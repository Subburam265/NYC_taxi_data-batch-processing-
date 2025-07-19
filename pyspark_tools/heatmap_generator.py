from pyspark.sql import functions as F

def get_heatmap(df, x_axis, y_axis, value_col, agg_func="avg"):
    """
    Generates a heatmap-style pivot table using PySpark's pivot operation.

    Parameters:
        df          : PySpark DataFrame
        x_axis      : Column to use for X-axis (pivoted columns)
        y_axis      : Column to use for Y-axis (grouped rows)
        value_col   : Column to aggregate
        agg_func    : Aggregation function: 'avg', 'sum', 'count'
        output_path : (Optional) Path to save the output CSV (must be a file:// path)
    
    Returns:
        heatmap_df  : Pivoted PySpark DataFrame
    """
    agg_map = {
        "avg": F.avg,
        "sum": F.sum,
        "count": F.count
    }

    if agg_func not in agg_map:
        raise ValueError(f"Unsupported agg_func '{agg_func}'. Use one of: {list(agg_map.keys())}")

    # Group and aggregate	
    df_heatmap = df.groupBy(y_axis).pivot(x_axis).agg(agg_map[agg_func](value_col).alias("value"))
	
    output_path = f"/NYC_taxi_data_batch_processing/data/output_data/heat_map/heatmap-(x_axis:{x_axis},y_axis:{y_axis},values:{value_col})"

    df_heatmap.write.mode("overwrite").csv(output_path, header=True)

    return df_heatmap

