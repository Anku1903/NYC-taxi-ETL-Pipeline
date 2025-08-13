Notebook: 01_ingest_raw_data.py
Purpose: Ingest raw NYC taxi data from Azure Data Lake or external sources into Databricks bronze table.
Author: Senior Data Engineer
"""








Notebook: 01_ingest_raw_data.py
Purpose: Robust ingestion of raw NYC taxi data into Databricks bronze table with parameterization, error handling, logging, and modular design.
Author: Senior Data Engineer
"""

"""
Notebook: 01_ingest_raw_data.py
Purpose: Robust ingestion of raw NYC taxi data into Databricks bronze table with parameterization, error handling, logging, and modular design.
Author: Senior Data Engineer
"""

from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import col
import sys
import os

# Logging utility
try:
    from src.databricks.utils.logging import log_info, log_error
except ImportError:
    def log_info(msg): print(f"INFO: {msg}")
    def log_error(msg): print(f"ERROR: {msg}")

# Configurations (use environment variables or widgets for production)
RAW_DATA_PATH = os.getenv("RAW_DATA_PATH", "abfss://raw@<datalake_name>.dfs.core.windows.net/nyc_taxi/yellow_tripdata_2023-01.csv.gz")
BRONZE_PATH = os.getenv("BRONZE_PATH", "abfss://bronze@<datalake_name>.dfs.core.windows.net/nyc_taxi/")

# Initialize Spark session
spark = SparkSession.builder.appName("NYC_Taxi_Ingest_Raw").getOrCreate()

# Expected schema columns
EXPECTED_COLUMNS = [
    "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count",
    "trip_distance", "RatecodeID", "store_and_fwd_flag", "PULocationID", "DOLocationID",
    "payment_type", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
    "improvement_surcharge", "total_amount", "congestion_surcharge"
]

def validate_schema(df, expected_cols):
    missing_cols = set(expected_cols) - set(df.columns)
    if missing_cols:
        log_error(f"Missing columns in raw data: {missing_cols}")
        raise ValueError(f"Missing columns: {missing_cols}")
    log_info("Schema validation passed.")

def ingest_raw_data():
    try:
        log_info(f"Reading raw data from {RAW_DATA_PATH}")
        raw_df = spark.read.option("header", True).csv(RAW_DATA_PATH)
        validate_schema(raw_df, EXPECTED_COLUMNS)
        log_info(f"Writing raw data to bronze path: {BRONZE_PATH}")
        raw_df.write.format("delta").mode("overwrite").save(BRONZE_PATH)
        log_info("Raw data ingested to bronze table successfully.")
    except AnalysisException as ae:
        log_error(f"Spark AnalysisException: {ae}")
        sys.exit(1)
    except Exception as e:
        log_error(f"General Exception: {e}")
        sys.exit(1)

ingest_raw_data()
