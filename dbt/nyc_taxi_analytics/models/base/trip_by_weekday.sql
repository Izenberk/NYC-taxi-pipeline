SELECT
    EXTRACT(DOW FROM tpep_pickup_datetime) AS weekday,
    COUNT(*) AS total_trips
FROM public.yellow_tripdata_2023_01
GROUP BY weekday
ORDER BY weekday
