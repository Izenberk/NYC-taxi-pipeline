SELECT
    EXTRACT(DOW FROM pickup_datetime) AS weekday,
    COUNT(*) AS total_trips
FROM {{ ref('stg_yellow_tripdata')}}
GROUP BY weekday
ORDER BY weekday
