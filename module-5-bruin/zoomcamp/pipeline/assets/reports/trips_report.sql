/* @bruin

name: reports.trips_report
type: duckdb.sql

depends:
  - staging.trips

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_date
  time_granularity: date

columns:
  - name: pickup_date
    type: date
    description: "Date of the trip"
    primary_key: true
    checks:
      - name: not_null
  - name: taxi_type
    type: string
    description: "Type of taxi: yellow or green"
    primary_key: true
    checks:
      - name: not_null
  - name: payment_type_name
    type: string
    description: "Human-readable payment type name"
    primary_key: true
  - name: total_trips
    type: bigint
    description: "Total number of trips in the group"
    checks:
      - name: non_negative
  - name: total_passengers
    type: float
    description: "Sum of passengers across all trips"
    checks:
      - name: non_negative
  - name: total_distance
    type: float
    description: "Total trip distance in miles"
    checks:
      - name: non_negative
  - name: total_fare_amount
    type: float
    description: "Sum of base fares in USD"
    checks:
      - name: non_negative
  - name: total_amount
    type: float
    description: "Sum of total amounts including all fees"
    checks:
      - name: non_negative

@bruin */

SELECT
    CAST(pickup_datetime AS DATE)              AS pickup_date,
    taxi_type,
    COALESCE(payment_type_name, 'unknown')     AS payment_type_name,
    COUNT(*)                                   AS total_trips,
    SUM(passenger_count)                       AS total_passengers,
    SUM(trip_distance)                         AS total_distance,
    SUM(fare_amount)                           AS total_fare_amount,
    SUM(total_amount)                          AS total_amount
FROM staging.trips
WHERE pickup_datetime >= TIMESTAMP '{{ start_datetime }}' AT TIME ZONE 'UTC'
  AND pickup_datetime < TIMESTAMP '{{ end_datetime }}' AT TIME ZONE 'UTC'
GROUP BY
    CAST(pickup_datetime AS DATE),
    taxi_type,
    COALESCE(payment_type_name, 'unknown')
