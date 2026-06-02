from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("SilverLayer") \
    .master("local[*]") \
    .getOrCreate()

# Read Bronze data
bronze_df = spark.read.parquet("data/parquet")

# Business rules
silver_df = bronze_df.filter(
    (col("status") != "CANCELLED") &
    (col("amount") > 1000)
)

# Write Silver data
silver_df.write.mode("overwrite").parquet("data/silver")

print("Silver layer created successfully!")

silver_df.show(20, False)

spark.stop()