-- V003: NYC sales tax is 8.875%, a share of 0.08875, which numeric(10,4) silently rounds to 0.0888.
-- Widening keeps every stored value and rewrites nothing; fare_rules is not published to Debezium.

ALTER TABLE fare_rules ALTER COLUMN amount TYPE numeric(12,6);
