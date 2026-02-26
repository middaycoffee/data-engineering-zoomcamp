/* @bruin

name: staging.trips
type: duckdb.sql

depends:
  - ingestion.trips
  - ingestion.payment_lookup

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp

columns:
  - name: pickup_datetime
    type: timestamp
    description: "When the trip started"
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: dropoff_datetime
    type: timestamp
    description: "When the trip ended"
    primary_key: true
    checks:
      - name: not_null
  - name: pu_location_id
    type: integer
    description: "Pickup location ID"
    primary_key: true
    checks:
      - name: not_null
  - name: do_location_id
    type: integer
    description: "Dropoff location ID"
    primary_key: true
    checks:
      - name: not_null
  - name: fare_amount
    type: float
    description: "Base fare amount in USD"
    primary_key: true
    checks:
      - name: non_negative
  - name: taxi_type
    type: string
    description: "Type of taxi: yellow or green"
    checks:
      - name: not_null
  - name: payment_type_name
    type: string
    description: "Human-readable payment type from lookup table"
  - name: airport_fee
    type: float
    description: "Airport fee (yellow taxi only, null for green)"
  - name: total_amount
    type: float
    description: "Total amount charged including all fees (can be negative for corrections/chargebacks)"

custom_checks:
  - name: no_duplicate_trips
    description: "No duplicate trips within the processed window"
    query: |
      SELECT COUNT(*) - COUNT(DISTINCT (pickup_datetime, dropoff_datetime, pu_location_id, do_location_id, fare_amount))
      FROM staging.trips
      WHERE pickup_datetime >= TIMESTAMP '{{ start_datetime }}' AT TIME ZONE 'UTC'
        AND pickup_datetime < TIMESTAMP '{{ end_datetime }}' AT TIME ZONE 'UTC'
    value: 0

@bruin */

WITH deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY pickup_datetime, dropoff_datetime, pu_location_id, do_location_id, fare_amount
            ORDER BY extracted_at DESC
        ) AS rn
    FROM ingestion.trips
    WHERE pickup_datetime >= TIMESTAMP '{{ start_datetime }}' AT TIME ZONE 'UTC'
      AND pickup_datetime < TIMESTAMP '{{ end_datetime }}' AT TIME ZONE 'UTC'
      AND pickup_datetime IS NOT NULL
      AND fare_amount >= 0
)
SELECT
    t.vendor_id,
    t.pickup_datetime,
    t.dropoff_datetime,
    t.passenger_count,
    t.trip_distance,
    t.ratecode_id,
    t.store_and_fwd_flag,
    t.pu_location_id,
    t.do_location_id,
    t.payment_type,
    p.payment_type_name,
    t.fare_amount,
    t.extra,
    t.mta_tax,
    t.tip_amount,
    t.tolls_amount,
    t.improvement_surcharge,
    t.total_amount,
    t.congestion_surcharge,
    t.airport_fee,
    t.taxi_type,
    t.extracted_at
FROM deduplicated t
LEFT JOIN ingestion.payment_lookup p
    ON t.payment_type = p.payment_type_id
WHERE t.rn = 1

