import pytest
from spark import *
import datetime

from pyspark.sql.functions import *
from chispa.dataframe_comparer import *


class TestFunctions(object):
    def test_exists(self):
        df = spark.createDataFrame(
            [(["a", "b", "c"],), (["x", "y", "z"],)], ["some_arr"]
        )
        equals_b = lambda e: e == "b"
        res = df.withColumn("has_b", exists(col("some_arr"), equals_b))
        expected = spark.createDataFrame(
            [(["a", "b", "c"], True), (["x", "y", "z"], False)], ["some_arr", "has_b"]
        )
        assert_df_equality(res, expected)

    def test_forall(self):
        df = spark.createDataFrame([([1, 2, 3],), ([2, 6, 12],)], ["some_arr"])
        is_even = lambda e: e % 2 == 0
        res = df.withColumn("all_even", forall(col("some_arr"), is_even))
        expected = spark.createDataFrame(
            [([1, 2, 3], False), ([2, 6, 12], True)], ["some_arr", "all_even"]
        )
        assert_df_equality(res, expected)

    def test_year(self):
        sourceDF = spark.createDataFrame(
            [("jose", datetime.date(2017, 1, 1)), ("li", datetime.date(2015, 7, 6))],
            ["name", "birthdate"],
        )
        actualDF = sourceDF.withColumn("birthyear", year(col("birthdate")))
        expectedDF = spark.createDataFrame(
            [
                ("jose", datetime.date(2017, 1, 1), 2017),
                ("li", datetime.date(2015, 7, 6), 2015),
            ],
            ["name", "birthdate"],
        )
        assert expectedDF.collect() == actualDF.collect()
