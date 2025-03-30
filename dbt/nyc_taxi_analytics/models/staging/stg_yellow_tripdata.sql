-- models/staging/stg_yellow_tripdata.sql

SELECT
    "VendorID" AS vendor_id,
    "tpep_pickup_datetime" AS pickup_datetime,
    "tpep_dropoff_datetime" AS dropoff_datetime,
    "passenger_count" AS passenger_count,
    "trip_distance" AS trip_distance,
    "RatecodeID" AS rate_code_id,
    "store_and_fwd_flag" AS store_and_fwd_flag,
    "PULocationID" AS pickup_location_id,
    "DOLocationID" AS dropoff_location_id,
    "payment_type" AS payment_type,
    "fare_amount" AS fare_amount,
    "extra" AS extra,
    "mta_tax" AS mta_tax,
    "tip_amount" AS tip_amount,
    "tolls_amount" AS tolls_amount,
    "improvement_surcharge" AS improvement_surcharge,
    "total_amount" AS total_amount,
    "congestion_surcharge" AS congestion_surcharge
FROM public.yellow_tripdata_2023_01

