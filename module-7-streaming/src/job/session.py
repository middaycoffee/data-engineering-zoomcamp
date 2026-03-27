import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

def log_processing():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1) 
    
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
        CREATE TABLE trip_count_session (
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            PULocationID INTEGER,
            num_trips BIGINT
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = 'trip_count_session',
            'username' = 'postgres',
            'password' = 'pass',
            'driver' = 'org.postgresql.Driver'
        );
    """

    t_env.execute_sql(source_ddl)
    t_env.execute_sql(sink_ddl)

    # The Session Window logic (groups by 5-minute inactivity gaps)
    t_env.execute_sql(
        """
        INSERT INTO trip_count_session
        SELECT 
            SESSION_START(pickup_ts, INTERVAL '5' MINUTE) as window_start,
            SESSION_END(pickup_ts, INTERVAL '5' MINUTE) as window_end,
            PULocationID,
            COUNT(*) as num_trips
        FROM green_trips
        GROUP BY 
            SESSION(pickup_ts, INTERVAL '5' MINUTE),
            PULocationID
        """
    ).wait()

if __name__ == '__main__':
    log_processing()