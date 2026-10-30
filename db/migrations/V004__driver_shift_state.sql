-- V004: what the supply model (ADR-0003) must remember between ticks, because the replayer is stateless.
-- status_changed_at already says how long an idle driver has idled or an offline one has rested.

ALTER TABLE drivers
  ADD COLUMN online_since timestamp,                      -- simulated time this shift began; null when offline
  ADD COLUMN leaving boolean NOT NULL DEFAULT false,      -- past the shift limit while busy: signs off at dropoff
  ADD CONSTRAINT drivers_online_since_check CHECK ((status = 'offline') = (online_since IS NULL)),
  ADD CONSTRAINT drivers_leaving_check CHECK (NOT leaving OR status IN ('en_route', 'on_trip'));
