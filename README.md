Project: Real-Time Order Streaming using Kafka and PySpark

Objective Build a real-time streaming pipeline that generates order
events, sends them to Kafka, and processes them using PySpark Structured
Streaming.

Architecture Python Producer ↓ Kafka Topic (orders) ↓ PySpark Consumer ↓
Console Output

Step 1: Start Kafka using Docker

docker run -d –name kafka -p 9092:9092 apache/kafka:latest

Verify: docker ps

Step 2: Create Kafka Topic

docker exec -it kafka /opt/kafka/bin/kafka-topics.sh –create –topic
orders –bootstrap-server localhost:9092

Verify: docker exec -it kafka /opt/kafka/bin/kafka-topics.sh –list
–bootstrap-server localhost:9092

Output: orders

Step 3: Build Kafka Producer

Created producer.py.

Purpose: - Generate order events - Convert events to JSON - Publish
messages to Kafka topic

Sample Event: { “order_id”: 4675, “customer_id”: 82, “amount”: 3213.26,
“status”: “DELIVERED”, “event_time”: “2026-06-01 15:40:25” }

Step 4: Install PySpark

pip install pyspark pip install kafka-python

Step 5: Build PySpark Consumer

Created consumer_pyspark.py

Consumer Actions: 1. Connect to Kafka 2. Subscribe to orders topic 3.
Read JSON messages 4. Parse JSON using schema 5. Display streaming data

Schema: - order_id - customer_id - amount - status - event_time

Step 6: Process Streaming Data

Used Spark Structured Streaming: spark.readStream

Output: writeStream.format(“console”)

Step 7: Verify Streaming Output

Example: Batch: 19

|3766|64|4705.49|CANCELLED|2026-06-01 15:40:31|

Technologies Used: - Python - Kafka - Docker - PySpark - Spark
Structured Streaming - JSON

Checkpointing was validated by stopping the consumer, allowing the producer to continue sending events, restarting the consumer, and confirming that Spark resumed from the last processed Kafka offset.

Key Learnings: - Kafka Producer Development - Kafka Topic Management -
Real-Time Event Streaming - Spark Structured Streaming - JSON Schema
Processing - Docker-based Kafka Setup

Resume One-Liner: Built a real-time data streaming pipeline using Kafka
and PySpark Structured Streaming to ingest, process, and analyze order
events in real time.
