-- Profile one full TLC high-volume FHV month against the contract's rules (contracts/tlc_trips.yaml).
-- Run: scripts/feasibility/sql.sh contracts/validation/profile_tlc_month.sql
-- Source: the January 2025 file loaded by feasibility test S10.
SELECT
  count(*)                                                                         AS rows,
  -- the key: how unique are the 5- and 6-column natural keys over the whole month?
  count(*) - count(DISTINCT hvfhs_license_num, request_datetime, pickup_datetime,
                            PULocationID, DOLocationID)                            AS dup_5col,
  count(*) - count(DISTINCT hvfhs_license_num, request_datetime, pickup_datetime,
                            dropoff_datetime, PULocationID, DOLocationID)          AS dup_6col,
  -- key inputs must never be null
  count_if(hvfhs_license_num IS NULL OR request_datetime IS NULL OR pickup_datetime IS NULL
           OR dropoff_datetime IS NULL OR PULocationID IS NULL OR DOLocationID IS NULL) AS key_input_nulls,
  -- the key serialises timestamps to whole seconds: any fractional seconds would be lost
  count_if(unix_micros(cast(request_datetime AS TIMESTAMP)) % 1000000 <> 0
        OR unix_micros(cast(pickup_datetime AS TIMESTAMP)) % 1000000 <> 0
        OR unix_micros(cast(dropoff_datetime AS TIMESTAMP)) % 1000000 <> 0)        AS fractional_seconds,
  -- providers
  count_if(hvfhs_license_num = 'HV0003')                                           AS uber,
  count_if(hvfhs_license_num = 'HV0005')                                           AS lyft,
  count_if(hvfhs_license_num NOT IN ('HV0003', 'HV0005'))                          AS other_provider,
  -- time rules
  count_if(on_scene_datetime IS NULL AND hvfhs_license_num = 'HV0005')             AS lyft_on_scene_null,
  count_if(on_scene_datetime IS NULL AND hvfhs_license_num = 'HV0003')             AS uber_on_scene_null,
  count_if(pickup_datetime < request_datetime AND hvfhs_license_num = 'HV0003')    AS uber_pickup_before_request,
  count_if(pickup_datetime < request_datetime AND hvfhs_license_num = 'HV0005')    AS lyft_pickup_before_request,
  count_if(dropoff_datetime <= pickup_datetime)                                    AS dropoff_not_after_pickup,
  count_if(trip_time <> unix_timestamp(dropoff_datetime) - unix_timestamp(pickup_datetime)) AS trip_time_mismatch,
  count_if(date_trunc('MONTH', pickup_datetime) <> TIMESTAMP '2025-01-01')         AS pickup_outside_month,
  -- zones
  count_if(PULocationID NOT BETWEEN 1 AND 265 OR DOLocationID NOT BETWEEN 1 AND 265) AS zone_out_of_range,
  count_if(PULocationID = 264 OR DOLocationID = 264)                               AS zone_264_unknown,
  count_if(DOLocationID = 265)                                                     AS dropoff_outside_nyc,
  -- distance and money
  count_if(trip_miles <= 0)                                                        AS miles_not_positive,
  max(trip_miles)                                                                  AS miles_max,
  count_if(trip_miles * 100 <> round(trip_miles * 100))                            AS miles_more_than_2dp,
  count_if(base_passenger_fare <= 0)                                               AS fare_not_positive,
  count_if(base_passenger_fare < 0)                                                AS fare_negative,
  count_if(driver_pay <= 0)                                                        AS pay_not_positive,
  count_if(tips < 0 OR tolls < 0 OR bcf < 0 OR sales_tax < 0 OR airport_fee < 0)   AS negative_passthrough,
  max(base_passenger_fare)                                                         AS fare_max,
  -- fee value sets
  array_sort(collect_set(congestion_surcharge))                                    AS congestion_values,
  array_sort(collect_set(airport_fee))                                             AS airport_fee_values,
  array_sort(collect_set(cbd_congestion_fee))                                      AS cbd_fee_values,
  count_if(cbd_congestion_fee > 0)                                                 AS cbd_fee_paid,
  count_if(cbd_congestion_fee IS NULL)                                             AS cbd_fee_null,
  -- money precision: can the money columns be stored exactly as DECIMAL(10,2)?
  count_if(base_passenger_fare * 100 <> round(base_passenger_fare * 100)
        OR driver_pay * 100 <> round(driver_pay * 100)
        OR tips * 100 <> round(tips * 100))                                        AS money_more_than_2dp,
  -- flags
  array_sort(collect_set(shared_request_flag))                                     AS shared_request_values,
  array_sort(collect_set(wav_match_flag))                                          AS wav_match_values
FROM ridepulse_dev.bronze.tlc_fhvhv_s10
