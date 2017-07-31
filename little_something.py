import unittest

from pyspark.sql import SparkSession

spark = SparkSession.builder \
  .master("local") \
  .appName("Word Count") \
  .config("spark.some.config.option", "some-value") \
  .getOrCreate()

class TestDataFrameMethods(unittest.TestCase):

    def test_count(self):
        df = spark.range(5)
        self.assertEqual(df.count(), 5)

if __name__ == '__main__':
    unittest.main()

