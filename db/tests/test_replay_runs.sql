-- V002: starting, resuming and jumping a replay, and which days count as fully replayed.
-- Runs in a throwaway database after every migration (db/tests/test_migrations.py).

-- 1. a first start must be on a day boundary
DO $$ BEGIN
  BEGIN
    PERFORM start_replay('2025-01-05 08:00', 10);
    RAISE EXCEPTION 'a start inside a day was accepted';
  EXCEPTION WHEN raise_exception THEN
    IF SQLERRM NOT LIKE 'a replay starts or jumps only at a day boundary%' THEN RAISE; END IF;
  END;
  BEGIN
    PERFORM start_replay(NULL, 10);
    RAISE EXCEPTION 'a resume with no run was accepted';
  EXCEPTION WHEN raise_exception THEN
    IF SQLERRM NOT LIKE 'nothing to resume%' THEN RAISE; END IF;
  END;
END $$;

-- 2. start on Jan 5; the clock sits at the start
DO $$ DECLARE r bigint; BEGIN
  r := start_replay('2025-01-05', 10);
  ASSERT (SELECT watermark FROM replay_state) = '2025-01-05', 'watermark at the start';
  ASSERT (SELECT count(*) FROM replay_runs) = 1, 'one run';
  ASSERT start_replay(NULL, 60) = r, 'resume returns the same run';
  ASSERT (SELECT speed FROM replay_state) = 60 AND (SELECT speed FROM replay_runs) = 60, 'resume sets the speed';
END $$;

-- 3. the replayer advances to Jan 8 06:00: Jan 6 and 7 are fully replayed (Jan 5 is the warm-up day,
--    Jan 8 needs Jan 9 06:00)
SELECT advance_watermark('2025-01-08 06:00');
DO $$ BEGIN
  ASSERT (SELECT array_agg(day ORDER BY day) FROM replayed_days) = ARRAY['2025-01-06', '2025-01-07']::date[],
         'replayed days after Jan 8 06:00';
END $$;

-- 4. going back is refused; a jump must land on a day boundary
DO $$ BEGIN
  BEGIN
    PERFORM start_replay('2025-01-06', 10);
    RAISE EXCEPTION 'a start before the watermark was accepted';
  EXCEPTION WHEN raise_exception THEN
    IF SQLERRM NOT LIKE 'simulated time only moves forward%' THEN RAISE; END IF;
  END;
  BEGIN
    PERFORM start_replay('2025-01-20 12:00', 10);
    RAISE EXCEPTION 'a jump inside a day was accepted';
  EXCEPTION WHEN raise_exception THEN
    IF SQLERRM NOT LIKE 'a replay starts or jumps only at a day boundary%' THEN RAISE; END IF;
  END;
END $$;

-- 5. jump to Jan 20: the first run stops at Jan 8 06:00 and keeps its days; Jan 8-19 are skipped;
--    the new run's days appear once its coverage allows
DO $$ BEGIN
  PERFORM start_replay('2025-01-20', 10);
  ASSERT (SELECT ended_at FROM replay_runs WHERE started_from = '2025-01-05') = '2025-01-08 06:00', 'old run ended';
  ASSERT (SELECT watermark FROM replay_state) = '2025-01-20', 'clock jumped';
  PERFORM advance_watermark('2025-01-22 07:00');
  ASSERT (SELECT array_agg(day ORDER BY day) FROM replayed_days)
         = ARRAY['2025-01-06', '2025-01-07', '2025-01-21']::date[], 'skipped days are not replayed days';
END $$;

-- 6. Debezium sees runs: replay_runs is in the publication
DO $$ BEGIN
  ASSERT EXISTS (SELECT 1 FROM pg_publication_tables WHERE pubname = 'ridepulse_cdc' AND tablename = 'replay_runs'),
         'replay_runs published';
END $$;

\echo 'all replay run checks passed'
