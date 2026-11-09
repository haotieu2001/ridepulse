-- Incidents reach the lakehouse by CDC (#79), so the rules' scoring (#25) can read their labels from silver
-- instead of Neon. Its existing rows arrive by an incremental snapshot (V007's signal table):
-- docs/runbooks/onboard-cdc-table.md. config/cdc_tables.json has its entry.
ALTER PUBLICATION ridepulse_cdc ADD TABLE incidents;
