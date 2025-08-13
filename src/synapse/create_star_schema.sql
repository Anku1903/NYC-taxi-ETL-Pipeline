
CREATE VIEW vw_fact_trip AS
SELECT * FROM dbo.fact_trip;

CREATE VIEW vw_dim_date AS
SELECT * FROM dbo.dim_date;

CREATE VIEW vw_dim_vendor AS
SELECT * FROM dbo.dim_vendor;

CREATE VIEW vw_dim_ratecode AS
SELECT * FROM dbo.dim_ratecode;

CREATE VIEW vw_dim_paymenttype AS
SELECT * FROM dbo.dim_paymenttype;

-- Example: Trips by vendor and month
CREATE VIEW vw_trips_by_vendor_month AS
SELECT
    v.VendorID,
    d.year,
    d.month,
    COUNT(f.VendorID) AS total_trips,
    SUM(f.total_amount) AS total_revenue
FROM dbo.fact_trip f
JOIN dbo.dim_vendor v ON f.VendorID = v.VendorID
JOIN dbo.dim_date d ON f.tpep_pickup_datetime = d.date_time
GROUP BY v.VendorID, d.year, d.month;

-- Example: Revenue by payment type
CREATE VIEW vw_revenue_by_paymenttype AS
SELECT
    p.payment_type,
    SUM(f.total_amount) AS total_revenue,
    COUNT(f.payment_type) AS trip_count
FROM dbo.fact_trip f
JOIN dbo.dim_paymenttype p ON f.payment_type = p.payment_type
GROUP BY p.payment_type;
