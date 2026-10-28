-- S4: query the metric view the way Genie and dashboards do, with MEASURE().
SELECT pickup_zone_id,
       MEASURE(trips)            AS trips,
       ROUND(MEASURE(avg_base_fare), 2) AS avg_base_fare,
       ROUND(MEASURE(avg_trip_miles), 2) AS avg_trip_miles
FROM ridepulse_dev.bronze.mv_trips_s4
GROUP BY pickup_zone_id
ORDER BY trips DESC
LIMIT 5
