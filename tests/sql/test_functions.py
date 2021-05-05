import pytest
from spark import *
import datetime

from pyspark.sql.functions import *
from chispa.dataframe_comparer import *


def describe_concat():
    def it_can_combine_arrays():
        df = spark.createDataFrame(
            [(["a", "a", "b", "c"], ["c", "d"])], ["arr1", "arr2"]
        )
        res = df.withColumn("arr_concat", concat(col("arr1"), col("arr2")))
        # df.withColumn(
        #     "arr_concat_distinct", array_distinct(concat(col("arr1"), col("arr2")))
        # ).show()
        expected = spark.createDataFrame(
            [(["a", "a", "b", "c"], ["c", "d"], ["a", "a", "b", "c", "c", "d"])],
            ["arr1", "arr2", "arr_concat"],
        )
        assert_df_equality(res, expected)


def describe_except():
    def it_except_combines_arrays():
        df = spark.createDataFrame(
            [(["a", "a", "b", "c"], ["c", "d"])], ["arr1", "arr2"]
        )
        res = df.withColumn("arr_except", array_except(col("arr1"), col("arr2")))
        expected = spark.createDataFrame(
            [(["a", "a", "b", "c"], ["c", "d"], ["a", "b"])],
            ["arr1", "arr2", "arr_except"],
        )
        assert_df_equality(res, expected)


def describe_exists():
    def it_works_like_any():
        df = spark.createDataFrame(
            [(["a", "b", "c"],), (["x", "y", "z"],)], ["some_arr"]
        )
        equals_b = lambda e: e == "b"
        res = df.withColumn("has_b", exists(col("some_arr"), equals_b))
        expected = spark.createDataFrame(
            [(["a", "b", "c"], True), (["x", "y", "z"], False)], ["some_arr", "has_b"]
        )
        assert_df_equality(res, expected)


def describe_filter():
    def it_can_remove_odds_from_array():
        df = spark.createDataFrame(
            [([1, 2, 3, 5, 7],), ([2, 4, 9],)], ["some_arr"]
        )
        df.show()
        is_even = lambda x: x % 2 == 0
        res = df.withColumn("arr_evens", filter(col("some_arr"), is_even))
        res.show()
        # expected = spark.createDataFrame(
        #     [(["a", "b", "c"], True), (["x", "y", "z"], False)], ["some_arr", "has_b"]
        # )
        # assert_df_equality(res, expected)


def describe_forall():
    def it_works_like_all():
        df = spark.createDataFrame([([1, 2, 3],), ([2, 6, 12],)], ["some_arr"])
        is_even = lambda e: e % 2 == 0
        res = df.withColumn("all_even", forall(col("some_arr"), is_even))
        expected = spark.createDataFrame(
            [([1, 2, 3], False), ([2, 6, 12], True)], ["some_arr", "all_even"]
        )
        assert_df_equality(res, expected)


def describe_intersect():
    def it_intersect_combines_arrays():
        df = spark.createDataFrame(
            [(["a", "a", "b", "c"], ["c", "d"])], ["arr1", "arr2"]
        )
        res = df.withColumn("arr_intersect", array_intersect(col("arr1"), col("arr2")))
        expected = spark.createDataFrame(
            [(["a", "a", "b", "c"], ["c", "d"], ["c"])],
            ["arr1", "arr2", "arr_intersect"],
        )
        assert_df_equality(res, expected)


def describe_union():
    def it_union_combines_arrays():
        df = spark.createDataFrame(
            [(["a", "a", "b", "c"], ["c", "d"])], ["arr1", "arr2"]
        )
        res = df.withColumn("arr_union", array_union(col("arr1"), col("arr2")))
        expected = spark.createDataFrame(
            [(["a", "a", "b", "c"], ["c", "d"], ["a", "b", "c", "d"])],
            ["arr1", "arr2", "arr_union"],
        )
        assert_df_equality(res, expected)


def describe_year():
    def it_extracts_year_from_date():
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
