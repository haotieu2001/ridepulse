-- S8 teardown: an orphaned slot makes Neon keep WAL forever (and its storage is 1 GB), so drop
-- Debezium's slot and publication once the EC2 is destroyed and the slot is inactive.
SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots
WHERE slot_name = 'debezium_s8' AND NOT active;
DROP PUBLICATION IF EXISTS dbz_s8;
SELECT slot_name, slot_type, active FROM pg_replication_slots;
