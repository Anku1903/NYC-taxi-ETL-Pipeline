
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, count
from pyspark.sql.utils import AnalysisException
import sys
import os

# Logging utility
try:
    from src.databricks.utils.logging import log_info, log_error
except ImportError:
    def log_info(msg): print(f"INFO: {msg}")
    def log_error(msg): print(f"ERROR: {msg}")

# Configurations
GOLD_PATH = os.getenv("GOLD_PATH", "abfss://gold@<datalake_name>.dfs.core.windows.net/nyc_taxi/")
EXPORT_PATH = os.getenv("EXPORT_PATH", "abfss://export@<datalake_name>.dfs.core.windows.net/nyc_taxi/")

spark = SparkSession.builder.appName("NYC_Taxi_Star_Schema").getOrCreate()

def load_table(name):
    log_info(f"Loading {name} from gold path: {GOLD_PATH}{name}")
    return spark.read.format("delta").load(f"{GOLD_PATH}{name}")

def run_analytics(fact_trip, dim_date, dim_vendor):
    log_info("Running analytics: Total trips per vendor per month...")
    result_df = fact_trip.join(dim_vendor, "VendorID") \
        .join(dim_date, fact_trip["tpep_pickup_datetime"] == dim_date["date_time"]) \
        .groupBy("VendorID", year("tpep_pickup_datetime").alias("year"), month("tpep_pickup_datetime").alias("month")) \
        .agg(count("VendorID").alias("total_trips"))
    result_df.show()

def export_tables(tables):
    for name, df in tables.items():
        log_info(f"Exporting {name} to {EXPORT_PATH}{name}")
        df.write.mode("overwrite").parquet(f"{EXPORT_PATH}{name}")

def validate_and_export():
    try:
        fact_trip = load_table("fact_trip")
        dim_date = load_table("dim_date")
        dim_vendor = load_table("dim_vendor")
        dim_ratecode = load_table("dim_ratecode")
        dim_paymenttype = load_table("dim_paymenttype")
        run_analytics(fact_trip, dim_date, dim_vendor)
        export_tables({
            "fact_trip": fact_trip,
            "dim_date": dim_date,
            "dim_vendor": dim_vendor,
            "dim_ratecode": dim_ratecode,
            "dim_paymenttype": dim_paymenttype
        })
        log_info("Star schema validated and exported for Synapse Analytics.")
    except AnalysisException as ae:
        log_error(f"Spark AnalysisException: {ae}")
        sys.exit(1)
    except Exception as e:
        log_error(f"General Exception: {e}")
        sys.exit(1)

validate_and_export()
