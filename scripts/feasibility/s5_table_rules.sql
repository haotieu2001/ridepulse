-- S5: first corpus (rule snippets) with Change Data Feed, source for a Delta Sync index.
-- Paraphrased test snippets, not quoted rule text.
CREATE OR REPLACE TABLE ridepulse_dev.bronze.s5_rules
TBLPROPERTIES (delta.enableChangeDataFeed = true)
AS SELECT * FROM VALUES
  ('r1', '59D-22', 'High-volume for-hire services must pay drivers at least the minimum per-minute and per-mile rates set by the Commission.'),
  ('r2', '59D-21', 'High-volume services must limit the time drivers spend cruising empty in the Manhattan core.'),
  ('r3', '59A-23', 'Bases must not charge passengers more than the posted rates plus tolls actually paid.'),
  ('r4', '80-13',  'Drivers may not refuse a passenger with a service animal.'),
  ('r5', '59A-17', 'Accessible vehicles must be dispatched to passengers who request one within the service time standard.'),
  ('r6', '51-03',  'A congestion surcharge is a state fee added to trips that start, end or pass through the congestion zone in Manhattan.')
  AS t(chunk_id, rule_id, chunk_text)
