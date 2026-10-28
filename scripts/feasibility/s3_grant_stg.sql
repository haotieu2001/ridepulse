-- S3: the CI service principal may create schemas (and, as their owner, volumes) in stg only.
GRANT USE CATALOG, CREATE SCHEMA ON CATALOG ridepulse_stg TO `39a85987-a185-409c-b168-41a6b40cb8b1`
