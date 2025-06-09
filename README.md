# NYC Taxi ETL Pipeline - Azure Data Engineering Project 🚕

## Overview

This project implements an end-to-end ETL (Extract, Transform, Load) pipeline for processing NYC taxi trip data using Azure data engineering services. The pipeline extracts raw NYC taxi data from a public GitHub dataset, processes it through multiple stages (bronze, silver, gold), builds a star schema data warehouse, and visualizes the results in a Power BI dashboard. The project leverages Azure Data Factory, Azure Databricks, Azure Synapse Analytics, and Power BI to create a scalable and efficient data pipeline for analytics.

### Key Features
- **Data Ingestion**: Extracts NYC taxi data from a GitHub repository using Azure Data Factory.
- **Data Processing**: Transforms raw data into bronze, silver, and gold tables using Delta Live Tables (DLT) in Azure Databricks.
- **Data Warehousing**: Builds a star schema in Azure Synapse Analytics Serverless SQL Pool with dimension and fact tables.
- **Data Visualization**: Creates a Material Design-inspired Power BI dashboard for analytics and insights.

### NYC Taxi Data
The dataset contains NYC taxi trip records with fields such as:
- `VendorID`, `tpep_pickup_datetime`, `tpep_dropoff_datetime`, `passenger_count`, `trip_distance`
- `pickup_longitude`, `pickup_latitude`, `dropoff_longitude`, `dropoff_latitude`
- `RateCodeID`, `payment_type`, `fare_amount`, `total_amount`, and more.

## Project Architecture

The pipeline follows a layered architecture:
1. **Ingestion Layer**:
   - **Azure Data Factory**: Extracts data from a GitHub CSV file and loads it into Azure Data Lake Storage Gen2.
2. **Processing Layer**:
   - **Azure Databricks (Delta Live Tables)**: Processes data into bronze (raw), silver (cleaned), and gold (transformed) tables, creating a star schema with dimensions (`dim_date`, `dim_vendor`, `dim_ratecode`, `dim_paymenttype`) and a fact table (`fact_trip`).
3. **Storage Layer**:
   - **Azure Data Lake Storage Gen2**: Stores raw, processed, and transformed data.
4. **Data Warehouse Layer**:
   - **Azure Synapse Analytics Serverless SQL Pool**: Defines external tables for the star schema, enabling efficient analytical queries.
5. **Visualization Layer**:
   - **Power BI**: Visualizes insights with a Material Design-inspired dashboard, including metrics like total trips, total fares, and trip density maps.
