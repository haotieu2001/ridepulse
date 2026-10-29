-- V002: which simulated days the replayer actually covered (ADR-0004 §8).
-- Simulated time only moves forward. A replay starts, or jumps forward, only at a day boundary;
-- each start or jump is a run. The daily gate and the completeness reconciliation must ignore days
-- the stream never saw, so they read replay_runs (published to Debezium) with this rule:
--
--   day D is fully replayed  <=>  some run started at or before D - 1 day (a warm-up day: up to
--   48 min of D's requests fall on the previous evening) and that run's coverage reached D + 30 h.

CREATE TABLE replay_runs (
  run_id        bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  started_from  timestamp NOT NULL UNIQUE,      -- simulated time the run began replaying
  ended_at      timestamp,                      -- simulated watermark when a later run jumped ahead
  speed         numeric(6,2) NOT NULL CHECK (speed > 0),
  started_at    timestamptz NOT NULL DEFAULT now(),
  updated_at    timestamptz NOT NULL DEFAULT now(),
  CHECK (started_from = date_trunc('day', started_from)),
  CHECK (ended_at IS NULL OR ended_at >= started_from)
);
CREATE TRIGGER replay_runs_updated_at BEFORE UPDATE ON replay_runs FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- The only way to start, resume or jump a replay. from_ts NULL resumes the current run.
CREATE FUNCTION start_replay(from_ts timestamp, replay_speed numeric) RETURNS bigint
LANGUAGE plpgsql AS $$
DECLARE
  w timestamp;
  current_run bigint;
BEGIN
  LOCK TABLE replay_runs IN SHARE ROW EXCLUSIVE MODE;      -- one starter at a time
  SELECT watermark INTO w FROM replay_state WHERE id = 1 FOR UPDATE;
  SELECT run_id INTO current_run FROM replay_runs ORDER BY started_from DESC LIMIT 1;

  IF from_ts IS NULL OR from_ts = w THEN                  -- resume where the clock stands
    IF current_run IS NULL THEN
      RAISE EXCEPTION 'nothing to resume: start a replay at a day boundary first';
    END IF;
    UPDATE replay_state SET speed = replay_speed, updated_at = now() WHERE id = 1;
    UPDATE replay_runs SET speed = replay_speed WHERE run_id = current_run;
    RETURN current_run;
  END IF;
  IF w IS NOT NULL AND from_ts < w THEN
    RAISE EXCEPTION 'simulated time only moves forward (watermark %, asked %): re-run past days as a backfill', w, from_ts;
  END IF;
  IF from_ts <> date_trunc('day', from_ts) THEN
    RAISE EXCEPTION 'a replay starts or jumps only at a day boundary, not %', from_ts;
  END IF;

  UPDATE replay_runs SET ended_at = w WHERE run_id = current_run;   -- a jump: the old run stops here
  INSERT INTO replay_runs (started_from, speed) VALUES (from_ts, replay_speed) RETURNING run_id INTO current_run;
  PERFORM advance_watermark(from_ts);
  UPDATE replay_state SET speed = replay_speed, updated_at = now() WHERE id = 1;
  RETURN current_run;
END;
$$;

-- The rule above, for people and tests; Databricks applies the same rule to the CDC copies.
CREATE VIEW replayed_days AS
SELECT r.run_id, d::date AS day
FROM replay_runs r
CROSS JOIN replay_state s
CROSS JOIN LATERAL generate_series(
  r.started_from + interval '1 day',                                  -- after the warm-up day
  coalesce(r.ended_at, s.watermark) - interval '30 hours',            -- day D needs D + 30 h
  interval '1 day') AS d;

ALTER PUBLICATION ridepulse_cdc ADD TABLE replay_runs;
