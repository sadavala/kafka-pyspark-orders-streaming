from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ReadParquet") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read.parquet("data/parquet")
print("Total Bronze Records:", df.count())
df.show(20, False)

spark.stop()