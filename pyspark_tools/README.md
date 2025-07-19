# PySpark Tools
Core data processing scripts for the NYC Taxi pipeline.

## 🛠️ Scripts

### data_cleaning.py 
 - Data quality and cleaning functions
 - Filters garbage records, handles nulls, detects outliers

### heatmap_generator.py 
 - Analysis and visualization engine
 - Generates temporal and spatial heatmaps
 - Aggregates trip patterns by given input

### download_data.py 
 - Data acquisition utilities
 - Downloads raw NYC taxi data
 - Sets up initial data directory structure

### utils.py 
 - Common helper functions
 - Spark session management
 - File I/O utilities
 - Handles the transformation of the NYC Taxi data

## 📦 Dependencies

PySpark 3.0+
pandas, numpy
