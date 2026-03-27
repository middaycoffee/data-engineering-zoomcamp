import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

def log_processing():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1) # Required by the homework
    
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    source_ddl = """
        CREATE TABLE green_trips (
            lpep_pickup_datetime VARCHAR,
            lpep_dropoff_datetime VARCHAR,
            PULocationID INTEGER,
            DOLocationID INTEGER,
            passenger_count INTEGER,
            trip_distance DOUBLE,
            tip_amount DOUBLE,
            total_amount DOUBLE,
            pickup_ts AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
            WATERMARK FOR pickup_ts AS pickup_ts - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'topic' = 'green-trips',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        );
    """

    sink_ddl = """
        CREATE TABLE trip_count_by_zone (
            window_start TIMESTAMP(3),
            PULocationID INTEGER,
            num_trips BIGINT
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = 'trip_count_by_zone',
            'username' = 'postgres',
            'password' = 'pass',
            'driver' = 'org.postgresql.Driver'
        );
    """

    t_env.execute_sql(source_ddl)
    t_env.execute_sql(sink_ddl)

    # The 5-Minute Tumbling Window logic
    t_env.execute_sql(
        """
        INSERT INTO trip_count_by_zone
        SELECT 
            TUMBLE_START(pickup_ts, INTERVAL '5' MINUTE) as window_start,
            PULocationID,
            COUNT(*) as num_trips
        FROM green_trips
        GROUP BY 
            TUMBLE(pickup_ts, INTERVAL '5' MINUTE),
            PULocationID
        """
    ).wait()

if __name__ == '__main__':
    log_processing()