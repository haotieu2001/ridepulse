-- S9 incremental check: one update, two updates of the same row, an insert and a delete.
-- Each update bumps updated_at, the cursor the connector reads from.
UPDATE dispatch.driver_profiles SET rating = 4.70, updated_at = now() WHERE driver_id = 'D0002';
UPDATE dispatch.driver_profiles SET vehicle_type = 'suv', updated_at = now() WHERE driver_id = 'D0003';
UPDATE dispatch.driver_profiles SET vehicle_type = 'wav', updated_at = now() WHERE driver_id = 'D0003';
INSERT INTO dispatch.driver_profiles (driver_id, full_name, base_license, vehicle_type, rating)
VALUES ('D0006', 'Synthetic Driver Six', 'B03404', 'sedan', 4.88);
DELETE FROM dispatch.driver_profiles WHERE driver_id = 'D0005';
SELECT driver_id, vehicle_type, rating, updated_at FROM dispatch.driver_profiles ORDER BY driver_id;
