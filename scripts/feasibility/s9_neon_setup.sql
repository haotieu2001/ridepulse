-- S9: a small reference table in Neon for the query-based (cursor) pull, path C of the plan.
-- Synthetic drivers. updated_at is the cursor; the index keeps incremental reads cheap.
CREATE SCHEMA IF NOT EXISTS dispatch;

CREATE TABLE IF NOT EXISTS dispatch.driver_profiles (
  driver_id    text PRIMARY KEY,
  full_name    text NOT NULL,
  base_license text NOT NULL,
  vehicle_type text NOT NULL,
  rating       numeric(3,2),
  updated_at   timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS driver_profiles_updated_at_idx
  ON dispatch.driver_profiles (updated_at);

INSERT INTO dispatch.driver_profiles (driver_id, full_name, base_license, vehicle_type, rating) VALUES
  ('D0001', 'Synthetic Driver One',   'B03404', 'sedan', 4.91),
  ('D0002', 'Synthetic Driver Two',   'B03404', 'suv',   4.78),
  ('D0003', 'Synthetic Driver Three', 'B02510', 'sedan', 4.85),
  ('D0004', 'Synthetic Driver Four',  'B02510', 'wav',   4.96),
  ('D0005', 'Synthetic Driver Five',  'B02764', 'sedan', 4.66)
ON CONFLICT (driver_id) DO NOTHING;

SELECT count(*) AS rows, max(updated_at) AS max_cursor FROM dispatch.driver_profiles;
