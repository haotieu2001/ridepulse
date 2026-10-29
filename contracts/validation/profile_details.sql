-- Follow-ups to profile_tlc_month.sql: precision checks with a tolerance (the source columns are
-- binary doubles, so x * 100 is rarely an exact integer), and counts for the edge cases.
SELECT
  count_if(abs(base_passenger_fare * 100 - round(base_passenger_fare * 100)) > 1e-6
        OR abs(driver_pay * 100 - round(driver_pay * 100)) > 1e-6
        OR abs(tips * 100 - round(tips * 100)) > 1e-6
        OR abs(tolls * 100 - round(tolls * 100)) > 1e-6
        OR abs(bcf * 100 - round(bcf * 100)) > 1e-6
        OR abs(sales_tax * 100 - round(sales_tax * 100)) > 1e-6
        OR abs(airport_fee * 100 - round(airport_fee * 100)) > 1e-6
        OR abs(congestion_surcharge * 100 - round(congestion_surcharge * 100)) > 1e-6
        OR abs(cbd_congestion_fee * 100 - round(cbd_congestion_fee * 100)) > 1e-6) AS money_more_than_2dp,
  count_if(abs(trip_miles * 1000 - round(trip_miles * 1000)) > 1e-6)              AS miles_more_than_3dp,
  count_if(abs(trip_miles * 100 - round(trip_miles * 100)) > 1e-6
           AND hvfhs_license_num = 'HV0003')                                       AS uber_miles_more_than_2dp,
  count_if(congestion_surcharge = 5.5)                                             AS congestion_5_50,
  count_if(cbd_congestion_fee = 3.0)                                               AS cbd_fee_3_00,
  count_if(airport_fee NOT IN (0, 1.25, 2.5, 5.0))                                 AS airport_fee_unusual,
  max(base_passenger_fare + tolls + bcf + sales_tax + congestion_surcharge + airport_fee + tips
      + coalesce(cbd_congestion_fee, 0))                                           AS rider_total_max,
  percentile(unix_timestamp(request_datetime) - unix_timestamp(pickup_datetime), 0.5)
    FILTER (WHERE pickup_datetime < request_datetime)                              AS pickup_before_request_p50_s,
  percentile(unix_timestamp(request_datetime) - unix_timestamp(pickup_datetime), 0.99)
    FILTER (WHERE pickup_datetime < request_datetime)                              AS pickup_before_request_p99_s,
  percentile(abs(trip_time - (unix_timestamp(dropoff_datetime) - unix_timestamp(pickup_datetime))), 0.5)
    FILTER (WHERE trip_time <> unix_timestamp(dropoff_datetime) - unix_timestamp(pickup_datetime)) AS trip_time_gap_p50_s,
  max(unix_timestamp(dropoff_datetime) - unix_timestamp(pickup_datetime))          AS duration_max_s
FROM ridepulse_dev.bronze.tlc_fhvhv_s10
