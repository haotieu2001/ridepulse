-- The rows behind the 6-column natural-key duplicates: identical rows, or different trips that share
-- the key? Decides whether the contract dedups them or gives them a sequence suffix.
WITH k AS (
  SELECT *, count(*) OVER (PARTITION BY hvfhs_license_num, request_datetime, pickup_datetime,
                                         dropoff_datetime, PULocationID, DOLocationID) AS n
  FROM ridepulse_dev.bronze.tlc_fhvhv_s10
)
SELECT hvfhs_license_num, request_datetime, pickup_datetime, dropoff_datetime, PULocationID, DOLocationID,
       trip_miles, base_passenger_fare, driver_pay, tips, shared_match_flag
FROM k WHERE n > 1
ORDER BY request_datetime, pickup_datetime, base_passenger_fare
