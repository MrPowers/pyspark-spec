import pytest
from spark import *

from pyspark.sql.functions import col

class TestDataFrame(object):

    def test_columns(self):
      sourceDF = spark.createDataFrame([("jose", 1), ("li", 2), ("luisa", 3)], ["name", "age"])
      assert(sourceDF.columns == ["name", "age"])

    def test_dataframe_equality(self):
        people1 = [('Alice', 1)]
        p1 = spark.createDataFrame(people1)

        people2 = [('Alice', 1)]
        p2 = spark.createDataFrame(people2)

        assert(p1.collect() == p2.collect())

    def test_count(self):
        df = spark.range(5)
        assert(df.count() == 5)

