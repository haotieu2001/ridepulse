-- S8: after switching Debezium to decimal.handling.mode=string, change three trips' fares so
-- new events show how NUMERIC columns arrive now.
UPDATE dispatch.trips
SET base_passenger_fare = base_passenger_fare + 1.00, updated_at = now()
WHERE trip_key IN (SELECT trip_key FROM dispatch.trips ORDER BY trip_key LIMIT 3);
