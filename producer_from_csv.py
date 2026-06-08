import json
import time
import pandas as pd
from pathlib import Path
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

source_path = Path("data/source_data")

csv_files = sorted(source_path.glob("orders_*.csv"))

print(f"Total CSV files found: {len(csv_files)}")

for file in csv_files:
    print(f"\nReading file: {file}")

    df = pd.read_csv(file)

    print(f"Records in file: {len(df)}")

    for _, row in df.iterrows():
        order = {
            "order_id": None if pd.isna(row["order_id"]) else int(row["order_id"]),
            "customer_id": None if pd.isna(row["customer_id"]) else int(row["customer_id"]),
            "amount": None if pd.isna(row["amount"]) else float(row["amount"]),
            "status": None if pd.isna(row["status"]) else str(row["status"]),
            "event_time": None if pd.isna(row["event_time"]) else str(row["event_time"])
        }

        print("Sending:", order)

        producer.send("orders", order)

        time.sleep(1)

producer.flush()
producer.close()

print("\nAll CSV records sent to Kafka successfully.")