SELECT
    payment_type,
    COUNT(*) AS total_trips,
    ROUND(AVG(trip_distance)::numeric, 2) AS avg_distance
FROM public.yellow_tripdata_2023_01
GROUP BY payment_type
ORDER BY payment_type