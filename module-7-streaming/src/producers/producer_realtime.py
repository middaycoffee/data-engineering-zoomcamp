import random
import time
from datetime import datetime, timedelta

from kafka import KafkaProducer
from models import Ride, ride_serializer # Bring in your exact custom serializer!

# Top pickup locations from the actual NYC yellow taxi data.
PICKUP_LOCATIONS = [
    79, 107, 48, 132, 234, 148, 249, 68, 90, 263,
    138, 230, 161, 162, 170, 237, 239, 186, 164, 236
]

DROPOFF_LOCATIONS = PICKUP_LOCATIONS

def make_ride(delay_seconds=0):
    # 1. Calculate realistic pickup and dropoff times
    pickup_dt = datetime.now() - timedelta(seconds=delay_seconds)
    dropoff_dt = pickup_dt + timedelta(minutes=random.randint(5, 30))
    
    # 2. Format times as strings to match your Flink DDL perfectly
    pickup_str = pickup_dt.strftime('%Y-%m-%d %H:%M:%S')
    dropoff_str = dropoff_dt.strftime('%Y-%m-%d %H:%M:%S')
    
    # 3. Fill out ALL 8 fields required by your models.py Dataclass
    return Ride(
        lpep_pickup_datetime=pickup_str,
        lpep_dropoff_datetime=dropoff_str,
        PULocationID=random.choice(PICKUP_LOCATIONS),
        DOLocationID=random.choice(DROPOFF_LOCATIONS),
        passenger_count=random.randint(1, 4),
        trip_distance=round(random.uniform(0.5, 20.0), 2),
        tip_amount=round(random.uniform(0.0, 10.0), 2),
        total_amount=round(random.uniform(5.0, 100.0), 2)
    )

# 4. Initialize Producer using your existing serializer
server = 'localhost:9092'
producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=ride_serializer,
)

topic_name = 'green-trips'
count = 0

print("Sending live mock events (Ctrl+C to stop)...")
print()

try:
    while True:
        # ~20% chance of a late event (3-10 seconds old) to test Flink watermarks
        if random.random() < 0.2:
            delay = random.randint(3, 10)
            ride = make_ride(delay_seconds=delay)
            print(f"  LATE ({delay}s) -> PU={ride.PULocationID} ts={ride.lpep_pickup_datetime}")
        else:
            ride = make_ride()
            print(f"  on time   -> PU={ride.PULocationID} ts={ride.lpep_pickup_datetime}")

        producer.send(topic_name, value=ride)
        count += 1
        
        # Pause for half a second before sending the next taxi
        time.sleep(0.5)

except KeyboardInterrupt:
    producer.flush()
    print(f"\nSent {count} events")