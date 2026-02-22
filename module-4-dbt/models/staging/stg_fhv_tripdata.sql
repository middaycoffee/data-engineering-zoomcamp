{{ config(materialized='view') }}

SELECT
    dispatching_base_num,
    pickup_datetime,
    dropOff_datetime AS dropoff_datetime,
    PUlocationID AS pickup_location_id,
    DOlocationID AS dropoff_location_id,
    SR_Flag AS sr_flag,
    Affiliated_base_number AS affiliated_base_number
FROM {{ source('raw_data', 'fhv_tripdata') }}
WHERE dispatching_base_num IS NOT NULL