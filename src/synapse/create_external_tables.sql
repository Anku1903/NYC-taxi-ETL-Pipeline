
CREATE EXTERNAL DATA SOURCE nyc_taxi_datalake
WITH (
    LOCATION = 'abfss://export@<datalake_name>.dfs.core.windows.net/nyc_taxi/'
);

-- External file format for Parquet
CREATE EXTERNAL FILE FORMAT ParquetFileFormat
WITH (
    FORMAT_TYPE = PARQUET
);

-- External tables for star schema
CREATE EXTERNAL TABLE dbo.fact_trip (
    VendorID INT,
    tpep_pickup_datetime DATETIME2,
    tpep_dropoff_datetime DATETIME2,
    passenger_count INT,
    trip_distance FLOAT,
    RatecodeID INT,
    payment_type VARCHAR(20),
    fare_amount FLOAT,
    total_amount FLOAT
)
WITH (
    LOCATION = 'fact_trip/',
    DATA_SOURCE = nyc_taxi_datalake,
    FILE_FORMAT = ParquetFileFormat
);

CREATE EXTERNAL TABLE dbo.dim_date (
    date_time DATETIME2,
    year INT,
    month INT,
    day INT
)
WITH (
    LOCATION = 'dim_date/',
    DATA_SOURCE = nyc_taxi_datalake,
    FILE_FORMAT = ParquetFileFormat
);

CREATE EXTERNAL TABLE dbo.dim_vendor (
    VendorID INT
)
WITH (
    LOCATION = 'dim_vendor/',
    DATA_SOURCE = nyc_taxi_datalake,
    FILE_FORMAT = ParquetFileFormat
);

CREATE EXTERNAL TABLE dbo.dim_ratecode (
    RatecodeID INT
)
WITH (
    LOCATION = 'dim_ratecode/',
    DATA_SOURCE = nyc_taxi_datalake,
    FILE_FORMAT = ParquetFileFormat
);

CREATE EXTERNAL TABLE dbo.dim_paymenttype (
    payment_type VARCHAR(20)
)
WITH (
    LOCATION = 'dim_paymenttype/',
    DATA_SOURCE = nyc_taxi_datalake,
    FILE_FORMAT = ParquetFileFormat
);
