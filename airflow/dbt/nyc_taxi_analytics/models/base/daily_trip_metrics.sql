SELECT
	DATE(pickup_datetime) AS trip_date,
	COUNT(*) AS total_trips,
	ROUND(AVG(fare_amount)::numeric, 2) AS avg_fare
FROM {{ ref('stg_yellow_tripdata')}}
GROUP BY 1
ORDER BY 1
