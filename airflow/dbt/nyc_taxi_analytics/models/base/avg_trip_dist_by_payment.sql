SELECT
    payment_type,
    COUNT(*) AS total_trips,
    ROUND(AVG(trip_distance)::numeric, 2) AS avg_distance
FROM {{ ref('stg_yellow_tripdata')}}
GROUP BY payment_type
ORDER BY payment_type