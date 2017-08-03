import pytest
from spark import *

class TestDataFrame(object):

    def test_dataframe_equality(self):
        people1 = [('Alice', 1)]
        p1 = spark.createDataFrame(people1)

        people2 = [('Alice', 1)]
        p2 = spark.createDataFrame(people2)

        assert(p1.collect() == p2.collect())

    def test_count(self):
        df = spark.range(5)
        assert(df.count() == 5)

