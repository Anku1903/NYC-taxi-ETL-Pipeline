
SELECT COUNT(*) AS missing_pickup_datetime
FROM dbo.fact_trip
WHERE tpep_pickup_datetime IS NULL;

SELECT COUNT(*) AS missing_dropoff_datetime
FROM dbo.fact_trip
WHERE tpep_dropoff_datetime IS NULL;

-- Check for invalid passenger counts
SELECT COUNT(*) AS invalid_passenger_count
FROM dbo.fact_trip
WHERE passenger_count <= 0 OR passenger_count > 6;

-- Check for negative or zero trip distances
SELECT COUNT(*) AS invalid_trip_distance
FROM dbo.fact_trip
WHERE trip_distance <= 0;

-- Check for duplicate VendorID in dim_vendor
SELECT VendorID, COUNT(*) AS vendor_count
FROM dbo.dim_vendor
GROUP BY VendorID
HAVING COUNT(*) > 1;

-- Check for orphaned fact records (no matching dimension)
SELECT COUNT(*) AS orphaned_vendor
FROM dbo.fact_trip f
LEFT JOIN dbo.dim_vendor v ON f.VendorID = v.VendorID
WHERE v.VendorID IS NULL;
