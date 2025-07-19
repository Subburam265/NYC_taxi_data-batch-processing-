# Data
All datasets for the NYC Taxi pipeline.

## 📂 Directories
### enrichment_data/sample_data/ 
 - Reference datasets
 - Zone lookups, traffic patterns, weather data
 - Used to enrich core taxi trip records

### taxi_data/sample_data/
 - Core trip data
 - NYC Yellow Taxi records (Q1 2025)
 - Parquet format for fast processing

### output_data/
 - Pipeline results
 - Cleaned datasets, quality reports
 - Generated heatmaps and analysis outputs

## 🔄 Data Flow
Raw taxi data + enrichment data → cleaning pipeline → analysis outputs

## 📊 Sample Period
January - March 2025

# ⚠️ Note
### These directories contain the necessary data for processing, but due to the size of the source data, only sample data will be available. 
### You can download the NYC project by running the `pyspark_scripts/download_data.py` script in pyspark and get the full data in the respective directories.
