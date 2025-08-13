Notebook: 02_bronze_to_silver.py
Purpose: Clean and validate raw data from bronze table, apply business rules, and write to silver table.
Author: Senior Data Engineer
"""








"""
Notebook: 02_bronze_to_silver.py
Purpose: Advanced cleaning, validation, and enrichment of NYC taxi data from bronze to silver layer with modular functions, logging, and data quality checks.
Author: Senior Data Engineer
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, when, lit
from pyspark.sql.utils import AnalysisException
import sys
import os

# Logging utility
try:
    from src.databricks.utils.logging import log_info, log_error
except ImportError:
    def log_info(msg): print(f"INFO: {msg}")
    def log_error(msg): print(f"ERROR: {msg}")

# Data quality checks
try:
    from src.databricks.utils.data_quality_checks import check_missing_values, check_value_ranges
except ImportError:
    def check_missing_values(df, columns): pass
    def check_value_ranges(df, column, min_value, max_value): pass

# Configurations
BRONZE_PATH = os.getenv("BRONZE_PATH", "abfss://bronze@<datalake_name>.dfs.core.windows.net/nyc_taxi/")
SILVER_PATH = os.getenv("SILVER_PATH", "abfss://silver@<datalake_name>.dfs.core.windows.net/nyc_taxi/")

spark = SparkSession.builder.appName("NYC_Taxi_Bronze_to_Silver").getOrCreate()

def clean_and_enrich(bronze_df):
    # Remove invalid records
    df = bronze_df.filter(col("passenger_count") > 0)
    df = df.filter(col("trip_distance") > 0)
    # Convert timestamps
    df = df.withColumn("tpep_pickup_datetime", to_timestamp(col("tpep_pickup_datetime")))
    df = df.withColumn("tpep_dropoff_datetime", to_timestamp(col("tpep_dropoff_datetime")))
    # Fill missing payment_type
    df = df.withColumn("payment_type", when(col("payment_type").isNull(), lit("Unknown")).otherwise(col("payment_type")))
    # Add data source column for lineage
    df = df.withColumn("data_source", lit("bronze"))
    return df

def run_data_quality_checks(df):
    log_info("Running data quality checks...")
    check_missing_values(df, ["VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count", "trip_distance"])
    check_value_ranges(df, "passenger_count", 1, 6)
    check_value_ranges(df, "trip_distance", 0.1, 100)

def process_bronze_to_silver():
    try:
        log_info(f"Reading bronze data from {BRONZE_PATH}")
        bronze_df = spark.read.format("delta").load(BRONZE_PATH)
        silver_df = clean_and_enrich(bronze_df)
        run_data_quality_checks(silver_df)
        log_info(f"Writing cleaned data to silver path: {SILVER_PATH}")
        silver_df.write.format("delta").mode("overwrite").save(SILVER_PATH)
        log_info("Bronze data cleaned and written to silver table.")
    except AnalysisException as ae:
        log_error(f"Spark AnalysisException: {ae}")
        sys.exit(1)
    except Exception as e:
        log_error(f"General Exception: {e}")
        sys.exit(1)

process_bronze_to_silver()
