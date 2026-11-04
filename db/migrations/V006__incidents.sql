-- V006: injected incidents (#21). Each row is one incident and its simulated window: the replayer applies it while
-- ticking, and the row is the ground truth the rules' precision and recall are measured against (#25).

CREATE TABLE incidents (
  incident_id  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  kind         text NOT NULL CHECK (kind IN ('stadium', 'ping_outage', 'duplicate_pings', 'tunnel_pings', 'malformed_pings')),
  starts_at    timestamp NOT NULL,              -- simulated time
  ends_at      timestamp NOT NULL,
  params       jsonb NOT NULL DEFAULT '{}',     -- stadium: zone, factor; ping_outage: borough; the rest: share (, delay_minutes)
  created_at   timestamptz NOT NULL DEFAULT now(),
  CHECK (ends_at > starts_at)
);

-- A stadium incident's extra trips are copies of real ones; this says which, so reconciliation can leave them out.
ALTER TABLE trips ADD COLUMN incident_id bigint REFERENCES incidents (incident_id);

-- Tunnel pings: held back in the tick that made them, released by a later tick with their original device time.
-- Rows live only between those two ticks; the transactions that add and remove them make retries safe.
CREATE TABLE late_pings (
  driver_id   text NOT NULL,
  device_ts   timestamp NOT NULL,
  release_at  timestamp NOT NULL,
  ping        jsonb NOT NULL,
  PRIMARY KEY (driver_id, device_ts)
);
CREATE INDEX late_pings_release_idx ON late_pings (release_at);
