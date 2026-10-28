-- S8 replay: drive each sampled trip through the dispatch lifecycle, 4 changes per trip
-- (requested -> assigned -> picked_up -> completed), and measure the WAL Neon writes.
-- Run with: scripts/feasibility/neon.sh scripts/feasibility/s8_replay.sql
-- Note: one statement per stage, so each stage is one transaction for all trips; the real
-- replayer spreads them over time. The number of row change events is the same.
SELECT pg_current_wal_lsn() AS start_lsn \gset

INSERT INTO dispatch.trips (trip_key, hvfhs_license_num, pu_location_id, do_location_id,
                            request_datetime, status, updated_at)
SELECT trip_key, hvfhs_license_num, pu_location_id, do_location_id, request_datetime, 'requested', now()
FROM dispatch.trips_replay_input;

UPDATE dispatch.trips SET status = 'assigned', updated_at = now()
WHERE status = 'requested';

UPDATE dispatch.trips t SET status = 'picked_up', pickup_datetime = i.pickup_datetime, updated_at = now()
FROM dispatch.trips_replay_input i
WHERE t.trip_key = i.trip_key AND t.status = 'assigned';

UPDATE dispatch.trips t SET status = 'completed', dropoff_datetime = i.dropoff_datetime,
       trip_miles = i.trip_miles, base_passenger_fare = i.base_passenger_fare,
       driver_pay = i.driver_pay, updated_at = now()
FROM dispatch.trips_replay_input i
WHERE t.trip_key = i.trip_key AND t.status = 'picked_up';

SELECT count(*) AS trips,
       count(*) FILTER (WHERE status = 'completed') AS completed,
       pg_wal_lsn_diff(pg_current_wal_lsn(), :'start_lsn') AS wal_bytes
FROM dispatch.trips;
