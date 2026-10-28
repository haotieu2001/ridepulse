-- S9: what the query-based pipeline has ingested so far.
SELECT driver_id, vehicle_type, rating, updated_at
FROM ridepulse_dev.bronze.ref_driver_profiles
ORDER BY driver_id
