-- S8: 5% of the real trips requested Mon 2025-01-06 08:00-09:00 (the plan's replay sample rate),
-- shaped like dispatch.trips. The hash makes the sample deterministic.
SELECT
  sha2(concat_ws('|', hvfhs_license_num, request_datetime, PULocationID, DOLocationID, pickup_datetime), 256) AS trip_key,
  hvfhs_license_num,
  PULocationID AS pu_location_id,
  DOLocationID AS do_location_id,
  request_datetime,
  pickup_datetime,
  dropoff_datetime,
  round(trip_miles, 2)          AS trip_miles,
  round(base_passenger_fare, 2) AS base_passenger_fare,
  round(driver_pay, 2)          AS driver_pay
FROM ridepulse_dev.bronze.tlc_fhvhv_s10
WHERE request_datetime >= '2025-01-06 08:00:00' AND request_datetime < '2025-01-06 09:00:00'
  AND pmod(abs(hash(hvfhs_license_num, request_datetime, PULocationID, DOLocationID, pickup_datetime)), 20) = 0
