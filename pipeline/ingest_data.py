# ingestion script for ingesting a data, NYC Taxi Data in our case, part by part with iteration. So both we can see
# how the ingestion is going and don't process the big chunk of data at once.


import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm



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




def run():
    pg_user = "root"
    pg_pass = "root"
    pg_host = "localhost"
    pg_port = 5432
    pg_db = "nyc_taxi"

    year = 2019
    month = 10
    table_name = 'yellow_taxi_data'

    prefix = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow"
    url = f"{prefix}/yellow_tripdata_{year}-{month:02d}.csv.gz" 
    engine = create_engine(f"postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}")

    df_iter = pd.read_csv(
        url,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator = True,
        chunksize = 100000
    )

    first_iter = True
    for df_chunks in tqdm(df_iter):
        if first_iter:
            df_chunks.head(0).to_sql(
                name=table_name, 
                con=engine, 
                if_exists='replace'
            )
            first_iter = False
            
        df_chunks.to_sql(
            name= table_name,
            con=engine, 
            if_exists='append'
        )


if __name__ == '__main__':
    run()

