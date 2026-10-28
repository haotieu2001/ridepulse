-- S9 network check: read Neon from the serverless SQL warehouse through the foreign catalog.
SELECT driver_id, vehicle_type, rating, updated_at
FROM neon_dev_fc.dispatch.driver_profiles
ORDER BY driver_id
