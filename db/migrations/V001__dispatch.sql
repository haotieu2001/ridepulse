-- V001: the RidePulse dispatch database (Neon Postgres), as the replayer and Debezium need it.
-- Design: docs/design/erd.md. Tables are unqualified so the same file can be applied to a test
-- schema (SET search_path) before the real one.

-- --- reference data (pulled daily by Lakeflow Connect's query-based connector, path C) ---------

CREATE TABLE zones (
  zone_id       integer PRIMARY KEY CHECK (zone_id BETWEEN 1 AND 265),
  borough       text NOT NULL,
  zone          text NOT NULL,
  service_zone  text NOT NULL,
  updated_at    timestamptz NOT NULL DEFAULT now()
);

-- Dated fee rates (BCF share, airport fee, congestion surcharge, CBD fee): a new row on each real
-- change date, never an update in place, so history stays queryable.
CREATE TABLE fare_rules (
  rule_id     bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  fee         text NOT NULL CHECK (fee IN ('bcf', 'airport_fee', 'congestion_surcharge', 'cbd_fee', 'sales_tax')),
  amount      numeric(10,4) NOT NULL CHECK (amount >= 0),
  unit        text NOT NULL CHECK (unit IN ('usd', 'share_of_base_fare')),
  valid_from  date NOT NULL,
  valid_to    date,
  updated_at  timestamptz NOT NULL DEFAULT now(),
  CHECK (valid_to IS NULL OR valid_to > valid_from),
  UNIQUE (fee, valid_from)
);

-- --- synthetic drivers --------------------------------------------------------------------------

CREATE TABLE drivers (
  driver_id          text PRIMARY KEY,
  status             text NOT NULL DEFAULT 'offline'
                       CHECK (status IN ('offline', 'idle', 'en_route', 'on_trip')),
  zone_id            integer REFERENCES zones (zone_id),
  current_trip_key   text,
  status_changed_at  timestamp NOT NULL,              -- simulated time (NTZ), like the TLC timestamps
  updated_at         timestamptz NOT NULL DEFAULT now(),
  CHECK ((status IN ('en_route', 'on_trip')) = (current_trip_key IS NOT NULL)),
  CHECK (status = 'offline' OR zone_id IS NOT NULL)
);

-- 1:1 with drivers. Edited ~1%/simulated day, sometimes twice between pulls, and one profile is
-- hard-deleted on purpose: the planted changes that ADR-0005 compares across path B (CDC) and
-- path C (cursor). Deleting the profile leaves the driver and its trips; a driver with trips can't be
-- deleted at all (it goes offline instead), because trip history must keep its driver.
CREATE TABLE driver_profiles (
  driver_id     text PRIMARY KEY REFERENCES drivers (driver_id) ON DELETE CASCADE,
  full_name     text NOT NULL,
  base_license  text NOT NULL,
  vehicle_type  text NOT NULL CHECK (vehicle_type IN ('sedan', 'suv', 'wav', 'luxury')),
  rating        numeric(3,2) CHECK (rating BETWEEN 1 AND 5),
  joined_on     date NOT NULL,
  updated_at    timestamptz NOT NULL DEFAULT now()
);

-- --- trips: one row per replayed trip, moved forward through its lifecycle ------------------------
-- Not published to Debezium (sizing.md: it would double Neon egress); trip events leave through the
-- outbox. Only the last 48 simulated hours are kept; the replayer purges older rows.

CREATE TABLE trips (
  trip_key               text PRIMARY KEY,           -- contracts/trip_key.py
  provider_code          text NOT NULL CHECK (provider_code IN ('HV0002', 'HV0003', 'HV0004', 'HV0005')),
  pickup_zone_id         integer NOT NULL REFERENCES zones (zone_id),
  dropoff_zone_id        integer NOT NULL REFERENCES zones (zone_id),
  status                 text NOT NULL DEFAULT 'requested'
                           CHECK (status IN ('requested', 'assigned', 'started', 'completed')),
  driver_id              text REFERENCES drivers (driver_id),   -- RESTRICT: history keeps its driver
  requested_at           timestamp NOT NULL,         -- simulated time, the real TLC values
  assigned_at            timestamp,
  driver_arrived_at      timestamp,                  -- null where the source has none (Lyft before 2025)
  picked_up_at           timestamp,
  dropped_off_at         timestamp,
  trip_miles             numeric(8,3),
  base_fare              numeric(10,2),
  tolls                  numeric(10,2),
  bcf                    numeric(10,2),
  sales_tax              numeric(10,2),
  congestion_surcharge   numeric(10,2),
  airport_fee            numeric(10,2),
  cbd_fee                numeric(10,2),
  tips                   numeric(10,2),
  driver_pay             numeric(10,2),
  updated_at             timestamptz NOT NULL DEFAULT now(),
  -- each state requires the timestamps and values the state implies
  CHECK (status = 'requested' OR (driver_id IS NOT NULL AND assigned_at IS NOT NULL)),
  CHECK (status NOT IN ('started', 'completed') OR picked_up_at IS NOT NULL),
  CHECK (status <> 'completed' OR (dropped_off_at IS NOT NULL AND base_fare IS NOT NULL AND driver_pay IS NOT NULL)),
  CHECK (dropped_off_at IS NULL OR picked_up_at IS NULL OR dropped_off_at >= picked_up_at)
);
CREATE INDEX trips_requested_at_idx ON trips (requested_at);   -- hot-window purge
CREATE INDEX trips_open_by_status_idx ON trips (status) WHERE status <> 'completed';
CREATE INDEX trips_driver_idx ON trips (driver_id) WHERE driver_id IS NOT NULL;

ALTER TABLE drivers
  ADD CONSTRAINT drivers_current_trip_fk FOREIGN KEY (current_trip_key)
  REFERENCES trips (trip_key) ON DELETE SET NULL DEFERRABLE INITIALLY DEFERRED;

-- --- transactional outbox ------------------------------------------------------------------------
-- Rows are inserted and deleted in the same transaction: the WAL keeps the insert (Debezium's
-- Outbox Event Router turns it into a trips.v1 event) and the table stays empty. A primary key
-- can't stop a re-insert after the delete, so ids are deterministic: UUIDv5(trip_key, type), and
-- silver drops repeats. Column names are the Event Router's defaults.

CREATE TABLE outbox (
  id             uuid PRIMARY KEY,
  aggregatetype  text NOT NULL DEFAULT 'trip',
  aggregateid    text NOT NULL,
  type           text NOT NULL CHECK (type IN ('TripRequested', 'DriverAssigned', 'TripStarted', 'TripCompleted')),
  payload        jsonb NOT NULL,
  occurred_at    timestamp NOT NULL              -- simulated time of the business event
);

-- --- the replay watermark (a single row, never moves backwards) ----------------------------------

CREATE TABLE replay_state (
  id          smallint PRIMARY KEY DEFAULT 1 CHECK (id = 1),
  watermark   timestamp NOT NULL,                -- simulated time written so far
  speed       numeric(6,2) NOT NULL DEFAULT 10,
  updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE FUNCTION advance_watermark(new_watermark timestamp) RETURNS timestamp
LANGUAGE sql AS $$
  INSERT INTO replay_state (id, watermark) VALUES (1, new_watermark)
  ON CONFLICT (id) DO UPDATE
    SET watermark = greatest(replay_state.watermark, EXCLUDED.watermark), updated_at = now()
  RETURNING watermark;
$$;

-- --- updated_at maintained by the database (path C's cursor must not depend on the writer) -------

CREATE FUNCTION set_updated_at() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at := now();
  RETURN NEW;
END;
$$;

CREATE TRIGGER zones_updated_at BEFORE UPDATE ON zones FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER fare_rules_updated_at BEFORE UPDATE ON fare_rules FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER drivers_updated_at BEFORE UPDATE ON drivers FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER driver_profiles_updated_at BEFORE UPDATE ON driver_profiles FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trips_updated_at BEFORE UPDATE ON trips FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- B-tree indexes on the cursor column: without one, every incremental pull scans the table (S9 docs).
CREATE INDEX zones_updated_at_idx ON zones (updated_at);
CREATE INDEX fare_rules_updated_at_idx ON fare_rules (updated_at);
CREATE INDEX driver_profiles_updated_at_idx ON driver_profiles (updated_at);

-- --- what Debezium may read ----------------------------------------------------------------------
-- Explicit, so the egress decision lives in the schema: trips is deliberately absent.

CREATE PUBLICATION ridepulse_cdc FOR TABLE outbox, drivers, driver_profiles, replay_state;
