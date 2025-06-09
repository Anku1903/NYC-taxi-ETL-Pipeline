-- 1. Create external data source pointing to Azure Data Lake Storage Gen2
IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'NYCTaxiDataLake')
BEGIN
    CREATE EXTERNAL DATA SOURCE NYCTaxiDataLake
    WITH (
        LOCATION = 'abfss://synapsestore@ankurdataenggstorage.dfs.core.windows.net',
        CREDENTIAL = DATABASE_SCOPED_CREDENTIAL
    );
END

-- 2. Create external file format for CSV files
IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'CsvFormat')
BEGIN
    CREATE EXTERNAL FILE FORMAT CsvFormat
    WITH (
        FORMAT_TYPE = DELIMITEDTEXT,
        FORMAT_OPTIONS (
            FIELD_TERMINATOR = ',',
            STRING_DELIMITER = '"',
            FIRST_ROW = 2,
            USE_TYPE_DEFAULT = TRUE
        )
    );
END

-- 3. Create dimension table: Date
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dim_date')
BEGIN
    CREATE EXTERNAL TABLE dim_date
    (
        date_key INT,
        date DATE,
        year INT,
        month INT,
        day INT,
        day_of_week INT
    )
    WITH (
        LOCATION = 'nyc_taxi_gold/dim_date',
        DATA_SOURCE = NYCTaxiDataLake,
        FILE_FORMAT = CsvFormat
    );
END

-- 4. Create dimension table: Vendor
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dim_vendor')
BEGIN
    CREATE EXTERNAL TABLE dim_vendor
    (
        vendor_key INT,
        VendorID INT,
        vendor_name VARCHAR(50)
    )
    WITH (
        LOCATION = 'nyc_taxi_gold/dim_vendor',
        DATA_SOURCE = NYCTaxiDataLake,
        FILE_FORMAT = CsvFormat
    );
END

-- 5. Create dimension table: Ratecode
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dim_ratecode')
BEGIN
    CREATE EXTERNAL TABLE dim_ratecode
    (
        ratecode_key INT,
        RateCodeID INT,
        ratecode_description VARCHAR(100)
    )
    WITH (
        LOCATION = 'nyc_taxi_gold/dim_ratecode',
        DATA_SOURCE = NYCTaxiDataLake,
        FILE_FORMAT = CsvFormat
    );
END

-- 6. Create dimension table: Payment Type
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dim_paymenttype')
BEGIN
    CREATE EXTERNAL TABLE dim_paymenttype
    (
        paymenttype_key INT,
        payment_type INT,
        paymenttype_description VARCHAR(100)
    )
    WITH (
        LOCATION = 'nyc_taxi_gold/dim_paymenttype',
        DATA_SOURCE = NYCTaxiDataLake,
        FILE_FORMAT = CsvFormat
    );
END

-- 7. Create fact table: Trip
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'fact_trip')
BEGIN
    CREATE EXTERNAL TABLE fact_trip
    (
        vendor_key INT,
        date_key INT,
        ratecode_key INT,
        paymenttype_key INT,
        tpep_pickup_datetime DATETIME2,
        tpep_dropoff_datetime DATETIME2,
        passenger_count INT,
        trip_distance FLOAT,
        fare_amount FLOAT,
        total_amount FLOAT,
        pickup_longitude FLOAT,
        pickup_latitude FLOAT,
        dropoff_longitude FLOAT,
        dropoff_latitude FLOAT
    )
    WITH (
        LOCATION = 'nyc_taxi_gold/fact_trip',
        DATA_SOURCE = NYCTaxiDataLake,
        FILE_FORMAT = CsvFormat
    );
END