-- S9 network check: can serverless compute reach Neon? A foreign catalog plus one query answers it in seconds.
CREATE FOREIGN CATALOG IF NOT EXISTS neon_dev_fc USING CONNECTION neon_dev OPTIONS (database 'neondb')
