-- V005: the demo-window guardrail (requirements §9: windows <= 3 h a day at 10x, so <= 40 simulated days a month on
-- Neon's egress budget). Counted where every tick passes, in simulated minutes per wall-clock day (UTC), so neither
-- a manual run nor a higher speed can get around it. The tick refuses once the day's allowance is spent.

ALTER TABLE replay_state
  ADD COLUMN budget_day date,                                   -- the wall-clock day budget_used_min counts
  ADD COLUMN budget_used_min numeric(10,2) NOT NULL DEFAULT 0;  -- simulated minutes advanced on that day
