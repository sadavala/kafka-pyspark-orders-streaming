from pyspark.sql import SparkSession
from pyspark.sql.functions import desc
spark = SparkSession.builder \
    .appName("ReadDQResults") \
    .master("local[*]") \
    .getOrCreate()

dq_df = spark.read.parquet("data/dq_results")

print("DQ Results Count:", dq_df.count())

dq_df.orderBy(desc("batch_id")).show(20, False)

spark.stop()