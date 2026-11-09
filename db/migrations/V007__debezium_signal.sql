-- Debezium's signal table (#79): a row here asks Debezium for an incremental snapshot of named tables, read in chunks
-- while streaming goes on, so a table added to the publication gets its existing rows without a full re-snapshot
-- (`snapshot.mode=when_needed` only re-reads after a lost slot). Debezium sees the request through the WAL, so the
-- table is published; config/cdc_tables.json names it as `signal_table`.
--
--   INSERT INTO dispatch.debezium_signal (id, type, data)
--   VALUES (gen_random_uuid()::text, 'execute-snapshot', '{"data-collections": ["dispatch.<table>"], "type": "incremental"}');
--
-- docs/runbooks/onboard-cdc-table.md has the whole procedure.
CREATE TABLE debezium_signal (
  id   varchar(64) PRIMARY KEY,
  type varchar(32) NOT NULL,
  data text
);

ALTER PUBLICATION ridepulse_cdc ADD TABLE debezium_signal;
