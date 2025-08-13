# NYC Taxi ETL Pipeline - Azure Data Engineering Project 🚕

## Overview

This project implements an end-to-end ETL (Extract, Transform, Load) pipeline for processing NYC taxi trip data using Azure data engineering services. The pipeline extracts raw NYC taxi data from a public GitHub dataset, processes it through multiple stages (bronze, silver, gold), builds a star schema data warehouse, and visualizes the results in a Power BI dashboard. The project leverages Azure Data Factory, Azure Databricks, Azure Synapse Analytics, and Power BI to create a scalable and efficient data pipeline for analytics.

### NYC Taxi Data
The dataset contains NYC taxi trip records with fields such as:
- `VendorID`, `tpep_pickup_datetime`, `tpep_dropoff_datetime`, `passenger_count`, `trip_distance`
- `pickup_longitude`, `pickup_latitude`, `dropoff_longitude`, `dropoff_latitude`
- `RateCodeID`, `payment_type`, `fare_amount`, `total_amount`, and more.


## Table of Contents
1. [Architecture](#architecture)
2. [Folder Structure](#folder-structure)
3. [Pipeline Stages](#pipeline-stages)
   - [Data Ingestion](#data-ingestion)
   - [Data Processing](#data-processing)
   - [Data Warehousing](#data-warehousing)
   - [Analytics & Visualization](#analytics--visualization)
4. [Real-World Use Cases](#real-world-use-cases)
5. [Data Quality & Monitoring](#data-quality--monitoring)

---

## Architecture

The pipeline is organized in layers:
- **Ingestion**: Azure Data Factory extracts raw data from public sources and loads it into Azure Data Lake.
- **Processing**: Databricks notebooks transform data through bronze, silver, and gold layers, applying business rules and data quality checks.
- **Warehousing**: Synapse Analytics creates external tables and star schema views for efficient querying.
- **Visualization**: Power BI dashboard provides insights into trip trends, revenue, and payment types.

---

## Folder Structure

```
src/
   data_factory/
      pipelines/
      linked_services/
      datasets/
   databricks/
      notebooks/
      jobs/
      utils/
   synapse/
      sql/
   powerbi/
      nyc-dashboard.png
```

---

## Pipeline Stages

### Data Ingestion
- **Azure Data Factory**: 
   - Pipelines (`nyc_taxi_ingest.json`) orchestrate data extraction from GitHub and load into Data Lake.
   - Linked services and datasets define connections and data formats.

### Data Processing
- **Databricks Notebooks**:
   - `01_ingest_raw_data.py`: Ingests and validates raw data.
   - `02_bronze_to_silver.py`: Cleans and enriches data, applies business rules.
   - `03_silver_to_gold.py`: Builds star schema dimensions and fact tables.
   - `04_star_schema.py`: Validates schema, runs analytics, and exports for Synapse.

- **Jobs & Utils**:
   - `etl_job.json`: Orchestrates notebook execution.
   - `data_quality_checks.py`: Modular data quality functions.
   - `logging.py`: Centralized logging for pipeline monitoring.

### Data Warehousing
- **Synapse SQL**:
   - `create_external_tables.sql`: Defines external tables for star schema.
   - `create_star_schema.sql`: Creates views for analytics.
   - `data_quality.sql`: Data quality checks and validation queries.

### Analytics & Visualization
- **Power BI**:
   - `nyc-dashboard.png`: Dashboard visualizing trip metrics, revenue, and payment insights.


## Real-World Use Cases

- Automated data refresh and monitoring.
- Data anomaly detection and alerting.
- Historical trend analysis and predictive analytics.
- Role-based access and security for sensitive data.
- Scalable architecture for large datasets.


## Data Quality & Monitoring

- Data quality checks are automated in Databricks and Synapse.
- Logging and error handling are implemented throughout the pipeline.
- Scripts and SQL queries help monitor data integrity and completeness.

---
