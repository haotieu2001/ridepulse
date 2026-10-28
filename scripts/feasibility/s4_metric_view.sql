-- S4: can Free Edition create a metric view (Genie's semantic layer) over bronze?
CREATE OR REPLACE VIEW ridepulse_dev.bronze.mv_trips_s4
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: "S4 feasibility: January 2025 high-volume FHV trips"
source: ridepulse_dev.bronze.tlc_fhvhv_s10
dimensions:
  - name: pickup_zone_id
    expr: PULocationID
  - name: pickup_date
    expr: DATE(pickup_datetime)
  - name: provider
    expr: hvfhs_license_num
measures:
  - name: trips
    expr: COUNT(1)
  - name: avg_base_fare
    expr: AVG(base_passenger_fare)
  - name: total_driver_pay
    expr: SUM(driver_pay)
  - name: avg_trip_miles
    expr: AVG(trip_miles)
$$
