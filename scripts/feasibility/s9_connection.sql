-- S9: Unity Catalog connection to the dev Neon database (direct endpoint, not the pooler).
-- The password is read from the Databricks secret scope at query time, never stored here.
CREATE CONNECTION IF NOT EXISTS neon_dev TYPE postgresql
OPTIONS (
  host 'ep-rapid-silence-b40e5b79.c-6.us-east-2.aws.neon.tech',
  port '5432',
  user 'neondb_owner',
  password secret('ridepulse', 'neon-dev-password')
)
COMMENT 'RidePulse dev dispatch database (Neon), for Lakeflow Connect'
