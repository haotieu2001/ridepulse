-- How ref/cbd_zones.csv was made (2026-11-07): the TLC zones inside the Congestion Relief Zone (Manhattan south of and
-- including 60th Street), from the trips themselves. A trip that starts and ends in one zone pays the CBD fee only if
-- that zone is inside the CRZ, so a zone is in when nearly all its intra-zone trips paid. On the January 2025 file
-- (from 2025-01-05, when the fee began) the split is clean: 38 zones paid on 94-100% of their intra-zone trips, then a
-- gap to six that straddle 60th Street (Lincoln Square East 47%, Central Park 41%, Lincoln Square West 30%, Upper East
-- Side South 29%, Lenox Hill East 16%, Lenox Hill West 15%), which are partly outside and are left out.
--
-- To check against the MTA's own list, "MTA Central Business District Taxi Zones" (data.ny.gov yfdc-w5jh): the plan
-- counts 37 zones, this gives 38. Battery Park (12, one intra-zone trip) is the weakest evidence.
SELECT PULocationID AS zone_id, count(*) AS intra_zone_trips,
       round(avg(CASE WHEN cbd_congestion_fee > 0 THEN 1.0 ELSE 0.0 END), 4) AS paid_share
FROM ridepulse_dev.bronze.tlc_fhvhv_s10
WHERE PULocationID = DOLocationID AND pickup_datetime >= '2025-01-05'
GROUP BY PULocationID
HAVING paid_share > 0.9
ORDER BY zone_id
