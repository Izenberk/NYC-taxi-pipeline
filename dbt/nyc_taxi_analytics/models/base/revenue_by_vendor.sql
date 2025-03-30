SELECT
    "VendorID" as vendor_id,
    COUNT(*) AS total_trips,
    ROUND(SUM(fare_amount)::numeric, 2) AS total_revenue,
    ROUND(AVG(fare_amount)::numeric, 2) AS avg_fare
FROM public.yellow_tripdata_2023_01
GROUP BY vendor_id
ORDER BY vendor_id