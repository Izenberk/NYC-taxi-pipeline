SELECT
	DATE(tpep_pickup_datetime) AS trip_date,
	COUNT(*) AS total_trips,
	ROUND(AVG(fare_amount)::numeric, 2) AS avg_fare
FROM public.yellow_tripdata_2023_01 yt
GROUP BY 1
ORDER BY 1
