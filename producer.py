import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

statuses = ["PLACED", "SHIPPED", "CANCELLED", "DELIVERED"]

while True:
    order = {
        "order_id": random.randint(1000, 9999),
        "customer_id": random.randint(1, 100),
        "amount": round(random.uniform(100, 5000), 2),
        "status": random.choice(statuses),
        "event_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    producer.send("orders", order)
    print("Sent:", order)
    time.sleep(2)