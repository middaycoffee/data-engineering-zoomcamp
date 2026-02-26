"""@bruin

name: ingestion.trips
type: python
image: python:3.11
connection: duckdb-default

materialization:
  type: table
  strategy: append

columns:
  - name: vendor_id
    type: integer
    description: "Vendor identifier"
  - name: pickup_datetime
    type: timestamp
    description: "When the trip started (normalized from tpep_/lpep_pickup_datetime)"
    primary_key: true
    checks:
      - name: not_null
  - name: dropoff_datetime
    type: timestamp
    description: "When the trip ended (normalized from tpep_/lpep_dropoff_datetime)"
    primary_key: true
    checks:
      - name: not_null
  - name: passenger_count
    type: float
    description: "Number of passengers"
  - name: trip_distance
    type: float
    description: "Trip distance in miles"
  - name: ratecode_id
    type: float
    description: "Rate code ID"
  - name: store_and_fwd_flag
    type: string
    description: "Store and forward flag"
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
  - name: payment_type
    type: integer
    description: "Payment type code"
  - name: fare_amount
    type: float
    description: "Base fare amount"
    primary_key: true
    checks:
      - name: not_null
  - name: extra
    type: float
    description: "Extra charges"
  - name: mta_tax
    type: float
    description: "MTA tax"
  - name: tip_amount
    type: float
    description: "Tip amount"
  - name: tolls_amount
    type: float
    description: "Tolls amount"
  - name: improvement_surcharge
    type: float
    description: "Improvement surcharge"
  - name: total_amount
    type: float
    description: "Total amount charged"
  - name: congestion_surcharge
    type: float
    description: "Congestion surcharge"
  - name: taxi_type
    type: string
    description: "Type of taxi: yellow or green"
    checks:
      - name: not_null
  - name: extracted_at
    type: timestamp
    description: "Timestamp when this record was extracted"

@bruin"""

import io
import json
import os
from datetime import datetime, date

import pandas as pd
import requests
from dateutil.relativedelta import relativedelta

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/"

# Column renames to normalize taxi-type-specific and camelCase names
RENAME_MAP = {
    "tpep_pickup_datetime": "pickup_datetime",
    "tpep_dropoff_datetime": "dropoff_datetime",
    "lpep_pickup_datetime": "pickup_datetime",
    "lpep_dropoff_datetime": "dropoff_datetime",
    "VendorID": "vendor_id",
    "RatecodeID": "ratecode_id",
    "PULocationID": "pu_location_id",
    "DOLocationID": "do_location_id",
    "Airport_fee": "airport_fee",
}


def materialize():
    start_date = datetime.strptime(os.environ["BRUIN_START_DATE"], "%Y-%m-%d").date()
    end_date = datetime.strptime(os.environ["BRUIN_END_DATE"], "%Y-%m-%d").date()

    bruin_vars = json.loads(os.environ.get("BRUIN_VARS", "{}"))
    taxi_types = bruin_vars.get("taxi_types", ["yellow", "green"])

    extracted_at = datetime.utcnow()
    all_frames = []

    current = date(start_date.year, start_date.month, 1)
    while current < end_date:
        year_month = current.strftime("%Y-%m")
        for taxi_type in taxi_types:
            url = f"{BASE_URL}{taxi_type}_tripdata_{year_month}.parquet"
            print(f"Fetching {url}")
            response = requests.get(url, timeout=300)
            response.raise_for_status()

            df = pd.read_parquet(io.BytesIO(response.content))
            df = df.rename(columns=RENAME_MAP)
            # Strip timezone info so timestamps are stored as plain TIMESTAMP (UTC values, no tz label)
            for col in df.select_dtypes(include=["datetimetz"]).columns:
                df[col] = df[col].dt.tz_convert("UTC").dt.tz_localize(None)
            df["taxi_type"] = taxi_type
            df["extracted_at"] = extracted_at
            all_frames.append(df)

        current += relativedelta(months=1)

    if not all_frames:
        return pd.DataFrame()

    return pd.concat(all_frames, ignore_index=True)
