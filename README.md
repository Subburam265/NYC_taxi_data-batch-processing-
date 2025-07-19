# 🚖 NYC Taxi Trip Analyzer — Powered by PySpark & Visual Intelligence
Unlock hidden insights from NYC Yellow Taxi data using big data engineering pipelines and visual analytics.

--------------------------------------------------------------------------------------

## 🔍 Overview
This project analyzes NYC Yellow Taxi trip data using a batch-processing pipeline built with **PySpark** and **Hadoop**. The final output includes both **cleaned datasets** and **visual heatmaps** that highlight hidden trends in fare, tip behavior, and trip durations.
> **Stack**: PySpark · Pandas · Seaborn · Matplotlib · Hadoop  
> **Pipeline**: Extract ➝ Clean ➝ Enrich ➝ Loading ➝ Aggregate ➝ Visualize

## 🧠 Why It Matters
City transport datasets are notoriously messy. This project:
- Filters out garbage records (invalid durations, zero fare trips, etc.)
- Enriches data with derived metrics (`fare_per_mile`, `tip_percent`, etc.)
- Visualizes patterns across **days, hours, boroughs**
- Sets up a foundation for **machine learning** and **real-time streaming** use cases
- This lays the groundwork for use cases in urban planning, traffic optimization, or dynamic pricing models.

## 📂 Folder Structure
```text
NYC_taxi_data-batch-processing/
├── data/
│   ├── enrichment_data/       # Optional zone, traffic, and weather data
│   ├── taxi_data/             # Sample NYC tripdata (Parquet format)
│   └── output_data/           # Cleaned data and generated heatmaps
├── pyspark_tools/             # Core PySpark processing modules
│   ├── data_cleaning.py       # Main cleaning logic
│   ├── heatmap_generator.py   # Heatmap generation from enriched data
│   └── utils.py               # Helper functions
├── .gitignore
└── README.md 

## ✅ Features Implemented
| Feature | Status | Description |
|---------|--------|-------------|
| 🚫 Null & garbage filtering | ✅ | Removes invalid fare, distance, and duration records |
| 📊 Heatmap generator        | ✅ | Generates hour vs day heatmaps for fare, tip, duration, etc. |
| 🧠 Derived metrics          | ✅ | Calculates `fare_per_mile`, `tip_percent`, `trip_duration_min`, etc. |
| 🗃️ Efficient storage        | ✅ | Uses Hadoop for distributed storage and also data is stored as `.parquet` format for efficient loading/storage |

## ⚠️ Known Limitations
- Real-time traffic data not fully integrated due to missing geo spatial columns in nyc data.
- No UI/dashboard yet for visualization.

## 🚧 What's Coming Next
- 🔁 Traffic enrichment
- 🤖 ML models (fare prediction, tip classification)
- 🧠 Rush hour classifier
- 📈 Trend analysis dashboards (Plotly/Streamlit)

## ⚡ TL;DR
- Built an end-to-end PySpark pipeline to clean and analyze NYC Yellow Taxi data (2019).
- Generated heatmaps to visualize fare, tip, and trip trends by hour and day.
- Applied data engineering principles using Hadoop, Parquet, and PySpark UDFs.

--------------------------------------------------------------------------------------

Want to contribute or discuss improvements? DM me or raise an issue. I'm open to feedback, pull requests, and critique.
