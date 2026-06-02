from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("GoldLayer") \
    .master("local[*]") \
    .getOrCreate()

# Read Silver layer
silver_df = spark.read.parquet("data/silver")

# Business aggregation
gold_df = silver_df.groupBy("status").count()

# Show results
print("Gold Layer Metrics")
gold_df.show()

# Save Gold layer
gold_df.write.mode("overwrite").parquet("data/gold")

spark.stop()