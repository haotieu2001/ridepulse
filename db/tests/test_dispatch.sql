-- The guards the replayer relies on (V001). Every check is an ASSERT, so the first failure stops
-- the run. db/tests/test_migrations.py runs this in a throwaway database after applying every
-- migration with db/migrate.py, with search_path set to the migrated schema.

-- seed: two zones, one driver with a profile, one fee rule
INSERT INTO zones VALUES (132, 'Queens', 'JFK Airport', 'Airports'), (230, 'Manhattan', 'Times Sq/Theatre District', 'Yellow Zone');
INSERT INTO drivers (driver_id, status, zone_id, status_changed_at, online_since) VALUES ('D0001', 'idle', 230, '2025-01-06 07:55', '2025-01-06 07:00');
INSERT INTO driver_profiles VALUES ('D0001', 'Synthetic Driver One', 'B03404', 'sedan', 4.9, '2024-03-01');
INSERT INTO fare_rules (fee, amount, unit, valid_from) VALUES ('bcf', 0.0275, 'share_of_base_fare', '2024-01-01');

-- 1. insert once: the same trip twice leaves one row
INSERT INTO trips (trip_key, provider_code, pickup_zone_id, dropoff_zone_id, requested_at)
VALUES ('k1', 'HV0003', 230, 132, '2025-01-06 08:00:01') ON CONFLICT (trip_key) DO NOTHING;
INSERT INTO trips (trip_key, provider_code, pickup_zone_id, dropoff_zone_id, requested_at)
VALUES ('k1', 'HV0003', 230, 132, '2025-01-06 08:00:01') ON CONFLICT (trip_key) DO NOTHING;
DO $$ BEGIN ASSERT (SELECT count(*) FROM trips) = 1, 'insert once'; END $$;

-- 2 + 3. a guarded transition with its outbox event, inside one transaction, twice
CREATE TEMP TABLE outbox_seen (n int);
DO $$
DECLARE
  moved int;
  events int;
BEGIN
  FOR attempt IN 1..2 LOOP
    WITH t AS (
      UPDATE trips SET status = 'assigned', driver_id = 'D0001', assigned_at = '2025-01-06 08:00:30'
      WHERE trip_key = 'k1' AND status = 'requested'
      RETURNING trip_key
    ), ev AS (
      INSERT INTO outbox (id, aggregateid, type, payload, occurred_at)
      SELECT 'a8f5f167-f44f-5c6d-9b2b-7f0e2b5ad7a1', trip_key, 'DriverAssigned',
             '{"driver_id": "D0001"}', '2025-01-06 08:00:30'
      FROM t RETURNING 1
    )
    SELECT (SELECT count(*) FROM t), (SELECT count(*) FROM ev) INTO moved, events;
    INSERT INTO outbox_seen VALUES (events);
    DELETE FROM outbox WHERE id = 'a8f5f167-f44f-5c6d-9b2b-7f0e2b5ad7a1';
    IF attempt = 1 THEN
      ASSERT moved = 1 AND events = 1, 'first transition moves the trip and writes one event';
    ELSE
      ASSERT moved = 0 AND events = 0, 'a repeated transition changes nothing and writes no event';
    END IF;
  END LOOP;
END $$;
DO $$ BEGIN
  ASSERT (SELECT count(*) FROM outbox) = 0, 'the outbox is empty at rest';
  ASSERT (SELECT sum(n) FROM outbox_seen) = 1, 'exactly one event across both attempts';
  ASSERT (SELECT status FROM trips WHERE trip_key = 'k1') = 'assigned', 'state is assigned';
END $$;

-- 4. states that would be lies are rejected by the schema
DO $$ BEGIN
  BEGIN
    UPDATE trips SET status = 'completed' WHERE trip_key = 'k1';   -- no pickup, dropoff or fare
    RAISE EXCEPTION 'completed without dropoff was accepted';
  EXCEPTION WHEN check_violation THEN NULL;
  END;
  BEGIN
    UPDATE trips SET status = 'finished' WHERE trip_key = 'k1';    -- not a lifecycle state
    RAISE EXCEPTION 'unknown status was accepted';
  EXCEPTION WHEN check_violation THEN NULL;
  END;
  BEGIN
    UPDATE drivers SET status = 'on_trip' WHERE driver_id = 'D0001';   -- on a trip with no trip
    RAISE EXCEPTION 'on_trip without a trip was accepted';
  EXCEPTION WHEN check_violation THEN NULL;
  END;
  BEGIN
    INSERT INTO zones VALUES (300, 'Nowhere', 'Nowhere', 'N/A');     -- TLC zones are 1-265
    RAISE EXCEPTION 'zone 300 was accepted';
  EXCEPTION WHEN check_violation THEN NULL;
  END;
END $$;

-- a full, valid lifecycle completes, and the driver can point at the trip
UPDATE trips SET status = 'started', picked_up_at = '2025-01-06 08:04:00' WHERE trip_key = 'k1' AND status = 'assigned';
UPDATE drivers SET status = 'on_trip', current_trip_key = 'k1', status_changed_at = '2025-01-06 08:04:00' WHERE driver_id = 'D0001';
UPDATE trips SET status = 'completed', dropped_off_at = '2025-01-06 08:31:00', base_fare = 52.10, driver_pay = 41.30, tips = 5.00
WHERE trip_key = 'k1' AND status = 'started';
DO $$ BEGIN ASSERT (SELECT status FROM trips WHERE trip_key = 'k1') = 'completed', 'lifecycle completes'; END $$;

-- 5. the watermark only moves forward
SELECT advance_watermark('2025-01-06 09:00');
SELECT advance_watermark('2025-01-06 08:00');      -- an older value from a retried run
DO $$ BEGIN
  ASSERT (SELECT watermark FROM replay_state) = '2025-01-06 09:00', 'watermark is monotonic';
  ASSERT (SELECT count(*) FROM replay_state) = 1, 'one watermark row';
END $$;

-- 6. updated_at comes from the database, so path C's cursor sees every edit
DO $$
DECLARE before_ts timestamptz;
BEGIN
  SELECT updated_at INTO before_ts FROM driver_profiles WHERE driver_id = 'D0001';
  PERFORM pg_sleep(0.01);
  UPDATE driver_profiles SET rating = 4.8, updated_at = '2000-01-01' WHERE driver_id = 'D0001';  -- a writer can't fake it
  ASSERT (SELECT updated_at FROM driver_profiles WHERE driver_id = 'D0001') > before_ts, 'trigger sets updated_at';
END $$;

-- 7. Debezium's publication carries the outbox, driver and replay tables, and not trips
DO $$ BEGIN
  ASSERT (SELECT array_agg(tablename::text ORDER BY tablename) FROM pg_publication_tables WHERE pubname = 'ridepulse_cdc')
         = ARRAY['driver_profiles', 'drivers', 'outbox', 'replay_runs', 'replay_state'], 'publication tables';
END $$;

-- 8. the planted hard delete removes a profile only; a driver with trip history can't be deleted
DELETE FROM driver_profiles WHERE driver_id = 'D0001';
DO $$ BEGIN
  ASSERT (SELECT count(*) FROM driver_profiles) = 0, 'profile deleted';
  ASSERT (SELECT count(*) FROM drivers) = 1, 'driver row stays';
  ASSERT (SELECT driver_id FROM trips WHERE trip_key = 'k1') = 'D0001', 'trip keeps its driver';
  BEGIN
    DELETE FROM drivers WHERE driver_id = 'D0001';
    RAISE EXCEPTION 'a driver with trips was deleted';
  EXCEPTION WHEN foreign_key_violation THEN NULL;
  END;
END $$;

\echo 'all dispatch schema checks passed'
