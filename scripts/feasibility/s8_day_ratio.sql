-- S8: how many trips the whole replayed day has versus the replayed hour, to scale the
-- measured hour up to a simulated day (both at the same 5% sample rate).
SELECT
  count(*) FILTER (WHERE request_datetime >= '2025-01-06 08:00:00' AND request_datetime < '2025-01-06 09:00:00') AS hour_trips,
  count(*) AS day_trips,
  round(count(*) / count(*) FILTER (WHERE request_datetime >= '2025-01-06 08:00:00' AND request_datetime < '2025-01-06 09:00:00'), 2) AS day_over_hour
FROM ridepulse_dev.bronze.tlc_fhvhv_s10
WHERE request_datetime >= '2025-01-06' AND request_datetime < '2025-01-07'
