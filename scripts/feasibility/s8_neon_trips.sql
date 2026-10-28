-- S8: the dispatch trips table Debezium captures. One row per trip, updated through its lifecycle
-- (requested -> assigned -> picked_up -> completed), like the plan's replayer will do.
CREATE TABLE IF NOT EXISTS dispatch.trips (
  trip_key            text PRIMARY KEY,
  hvfhs_license_num   text NOT NULL,
  pu_location_id      int  NOT NULL,
  do_location_id      int  NOT NULL,
  request_datetime    timestamp NOT NULL,
  pickup_datetime     timestamp,
  dropoff_datetime    timestamp,
  trip_miles          numeric(8,2),
  base_passenger_fare numeric(8,2),
  driver_pay          numeric(8,2),
  status              text NOT NULL,
  updated_at          timestamptz NOT NULL DEFAULT now()
);
-- Staging for the replay: a 5% sample of one real TLC hour, loaded with \copy.
CREATE TABLE IF NOT EXISTS dispatch.trips_replay_input (LIKE dispatch.trips);
-- LIKE copies NOT NULL; staging rows have no lifecycle status yet.
ALTER TABLE dispatch.trips_replay_input ALTER COLUMN status DROP NOT NULL;
ALTER TABLE dispatch.trips_replay_input ALTER COLUMN updated_at DROP NOT NULL;
