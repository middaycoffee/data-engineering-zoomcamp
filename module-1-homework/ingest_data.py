# ingestion script for ingesting a data, NY Taxi Data in our case, part by part with iteration. So both we can see
# how the ingestion is going and don't process the big chunk of data at once.

import pandas as pd
from sqlalchemy import create_engine

dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

pg_user = "postgres"
pg_pass = "postgres"
pg_host = "localhost"
pg_port = 5433
pg_db = "ny_taxi"
chunksize = 100000
target_table_zone = "zones"
target_table_green = "green_taxi_trips"

def run():
    url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv" 
    engine = create_engine(f"postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}")

    df_zones = pd.read_csv(url)

    df_zones.to_sql(
        name=target_table_zone, 
        con=engine, 
        if_exists='replace'
    )

    url_green = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet"
    df_green = pd.read_parquet(url_green).to_sql(
            name= target_table_green,
            con=engine, 
            if_exists='replace'
        )


if __name__ == '__main__':
    run()
