from pyspark.sql import SparkSession

spark = SparkSession.builder \
  .master("local") \
  .appName("Word Count") \
  .config("spark.some.config.option", "some-value") \
  .getOrCreate()

df = spark.range(5)
df.show()