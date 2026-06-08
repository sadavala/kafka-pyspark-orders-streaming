from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit, current_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType
from pyspark.sql.functions import col, from_json, lit, current_timestamp, isnan

spark = SparkSession.builder \
    .appName("KafkaToBronzeWithDQ") \
    .master("local[*]") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0"
    ) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("order_id", IntegerType()),
    StructField("customer_id", IntegerType()),
    StructField("amount", DoubleType()),
    StructField("status", StringType()),
    StructField("event_time", StringType())
])

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "orders") \
    .option("startingOffsets", "latest") \
    .load()

orders_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")


def process_batch(batch_df, batch_id):
    source_count = batch_df.count()

    valid_df = batch_df.filter(
        col("order_id").isNotNull() &
        ~isnan(col("order_id")) &
        col("customer_id").isNotNull() &
        col("amount").isNotNull() &
        col("status").isNotNull()
    )

    bronze_count = valid_df.count()

    dq_status = "PASS" if source_count == bronze_count else "FAIL"

    print("====================================")
    print(f"Batch ID       : {batch_id}")
    print(f"Source Count   : {source_count}")
    print(f"Bronze Count   : {bronze_count}")
    print(f"DQ Status      : {dq_status}")
    print("====================================")

    valid_df.write.mode("append").parquet("data/bronze_dq_test")

    dq_row = [(batch_id, source_count, bronze_count, dq_status)]

    dq_df = spark.createDataFrame(
        dq_row,
        ["batch_id", "source_count", "bronze_count", "dq_status"]
    ).withColumn("created_at", current_timestamp())

    dq_df.write.mode("append").parquet("data/dq_results_test")


query = orders_df.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", "data/checkpoints_dq_test")     \
    .start()

query.awaitTermination()