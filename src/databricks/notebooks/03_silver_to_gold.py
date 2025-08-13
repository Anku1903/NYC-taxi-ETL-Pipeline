Notebook: 03_silver_to_gold.py
Purpose: Transform cleaned silver data into star schema dimensions and fact table (gold layer).
Author: Senior Data Engineer
"""
"""
Notebook: 03_silver_to_gold.py
Purpose: Advanced star schema transformation of NYC taxi data from silver to gold layer with modular functions, logging, and error handling.
Author: Senior Data Engineer
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth
from pyspark.sql.utils import AnalysisException
import sys
import os

# Logging utility
try:
    from src.databricks.utils.logging import log_info, log_error
except ImportError:
    def log_info(msg): print(f"INFO: {msg}")
    def log_error(msg): print(f"ERROR: {msg}")

SILVER_PATH = os.getenv("SILVER_PATH", "abfss://silver@<datalake_name>.dfs.core.windows.net/nyc_taxi/")
GOLD_PATH = os.getenv("GOLD_PATH", "abfss://gold@<datalake_name>.dfs.core.windows.net/nyc_taxi/")

spark = SparkSession.builder.appName("NYC_Taxi_Silver_to_Gold").getOrCreate()


def create_dim_date(df):
    return df.select(col("tpep_pickup_datetime").alias("date_time")) \
        .withColumn("year", year(col("date_time"))) \
        .withColumn("month", month(col("date_time"))) \
        .withColumn("day", dayofmonth(col("date_time"))) \
        .dropDuplicates()

def create_dim_vendor(df):
    return df.select("VendorID").dropDuplicates()

def create_dim_ratecode(df):
    return df.select("RatecodeID").dropDuplicates()

def create_dim_paymenttype(df):
    return df.select("payment_type").dropDuplicates()

def create_fact_trip(df):
    return df.select(
        "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count",
        "trip_distance", "RatecodeID", "payment_type", "fare_amount", "total_amount"
    )

def write_table(df, path, name):
    log_info(f"Writing {name} to gold path: {path}{name}")
    df.write.format("delta").mode("overwrite").save(f"{path}{name}")

def process_silver_to_gold():
    try:
        log_info(f"Reading silver data from {SILVER_PATH}")
        silver_df = spark.read.format("delta").load(SILVER_PATH)
        dim_date = create_dim_date(silver_df)
        dim_vendor = create_dim_vendor(silver_df)
        dim_ratecode = create_dim_ratecode(silver_df)
        dim_paymenttype = create_dim_paymenttype(silver_df)
        fact_trip = create_fact_trip(silver_df)
        for name, df in zip([
            "dim_date", "dim_vendor", "dim_ratecode", "dim_paymenttype", "fact_trip"
        ], [
            dim_date, dim_vendor, dim_ratecode, dim_paymenttype, fact_trip
        ]):
            write_table(df, GOLD_PATH, name)
        log_info("Silver data transformed and written to gold layer.")
    except AnalysisException as ae:
        log_error(f"Spark AnalysisException: {ae}")
        sys.exit(1)
    except Exception as e:
        log_error(f"General Exception: {e}")
        sys.exit(1)

process_silver_to_gold()
