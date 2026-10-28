-- S5: second corpus (policy snippets) with Change Data Feed, source for the second index.
CREATE OR REPLACE TABLE ridepulse_dev.bronze.s5_policies
TBLPROPERTIES (delta.enableChangeDataFeed = true)
AS SELECT * FROM VALUES
  ('p1', 'cancellation', 'Riders who cancel more than two minutes after a driver is assigned pay a cancellation fee.'),
  ('p2', 'lost_items',   'Report a lost item in the app within 72 hours; the driver is asked to return it for a return fee.'),
  ('p3', 'airport',      'Drivers wait in the airport holding lot until dispatched to the terminal pickup area.'),
  ('p4', 'payouts',      'Driver earnings are paid out weekly on Tuesday, covering the previous Monday to Sunday.'),
  ('p5', 'surge',        'Prices rise when ride requests outnumber available drivers in a zone, and fall as supply returns.')
  AS t(chunk_id, policy, chunk_text)
