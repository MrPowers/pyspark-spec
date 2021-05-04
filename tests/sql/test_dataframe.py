import pytest
from spark import *

from chispa.dataframe_comparer import *
from pyspark.sql.functions import col


def describe_columns():
    def it_returns_all_column_names():
        data = [("jose", 1), ("li", 2), ("luisa", 3)]
        sourceDF = spark.createDataFrame(data, ["name", "age"])
        assert sourceDF.columns == ["name", "age"]


def describe_corr():
    def it_computes_the_correlation():
        data = [(1, 10), (2, 15), (3, 33)]
        sourceDF = spark.createDataFrame(data, ["quiz1", "quiz2"])
        corr = sourceDF.corr("quiz1", "quiz2")
        assert pytest.approx(0.95, 0.1) == corr


def describe_count():
    def it_returns_row_count():
        df = spark.range(5)
        assert df.count() == 5


def describe_distinct():
    def it_returns_unique_rows():
        source_data = [("jose", 1), ("li", 2), ("jose", 1)]
        sourceDF = spark.createDataFrame(source_data, ["name", "age"])
        actualDF = sourceDF.distinct()
        expected_data = [("jose", 1), ("li", 2)]
        expectedDF = spark.createDataFrame(expected_data, ["name", "age"])
        assert sorted(expectedDF.collect()) == sorted(actualDF.collect())


def describe_drop_duplicates():
    def it_removes_duplicates():
        source_data = [("jose", 1), ("li", 2), ("jose", 1)]
        sourceDF = spark.createDataFrame(source_data, ["name", "age"])
        actualDF = sourceDF.drop_duplicates()
        expected_data = [("jose", 1), ("li", 2)]
        expectedDF = spark.createDataFrame(expected_data, ["name", "age"])
        assert sorted(expectedDF.collect()) == sorted(actualDF.collect())


def describe_join():
    def it_performs_default_join():
        peopleDF = spark.createDataFrame(
            [("larry", "1"), ("jeff", "2"), ("susy", "3")], ["person", "id"]
        )
        birthplaceDF = spark.createDataFrame(
            [("new york", "1"), ("ohio", "2"), ("los angeles", "3")],
            ["city", "person_id"],
        )
        actualDF = peopleDF.join(birthplaceDF, peopleDF.id == birthplaceDF.person_id)
        expectedDF = spark.createDataFrame(
            [
                ("larry", "1", "new york", "1"),
                ("jeff", "2", "ohio", "2"),
                ("susy", "3", "los angeles", "3"),
            ],
            ["person", "id", "city", "person_id"],
        )
        assert sorted(actualDF.collect()) == sorted(expectedDF.collect())


def describe_select():
    def it_works_with_array_argument():
        data = [("jose", 1, "mexico"), ("li", 2, "china"), ("sandy", 3, "usa")]
        source_df = spark.createDataFrame(data, ["name", "age", "country"])
        actual_df = source_df.select(["age", "name"])
        data = [(1, "jose"), (2, "li"), (3, "sandy")]
        expected_df = spark.createDataFrame(data, ["age", "name"])
        assert sorted(actual_df.collect()) == sorted(expected_df.collect())

    def it_works_with_multiple_string_arguments():
        data = [("jose", 1, "mexico"), ("li", 2, "china"), ("sandy", 3, "usa")]
        source_df = spark.createDataFrame(data, ["name", "age", "country"])
        actual_df = source_df.select("age", "name")
        data = [(1, "jose"), (2, "li"), (3, "sandy")]
        expected_df = spark.createDataFrame(data, ["age", "name"])
        assert sorted(actual_df.collect()) == sorted(expected_df.collect())


def describe_union():
    def it_combines_two_dataframes():
        americans = spark.createDataFrame(
            [("bob", 42), ("lisa", 59)], ["first_name", "age"]
        )
        colombians = spark.createDataFrame(
            [("maria", 20), ("camilo", 31)], ["first_name", "age"]
        )
        res = americans.union(colombians)
        expected = spark.createDataFrame(
            [("bob", 42), ("lisa", 59), ("maria", 20), ("camilo", 31)],
            ["first_name", "age"],
        )
        assert_df_equality(res, expected)
        # this is a bug that definitely should not exist
        # brasilans = spark.createDataFrame(
        #     [(33, "tiago"), (36, "lilly")], ["age", "first_name"]
        # )
        # brasilans.show()
        # americans.union(brasilans).show()
        # americans.printSchema()
        # brasilans.printSchema()


def desctibe_unionByName():
    def it_handles_columns_in_different_orders():
        americans = spark.createDataFrame(
            [("bob", 42), ("lisa", 59)], ["first_name", "age"]
        )
        brasilans = spark.createDataFrame(
            [(33, "tiago"), (36, "lilly")], ["age", "first_name"]
        )
        res = americans.unionByName(brasilans)
        expected = spark.createDataFrame(
            [("bob", 42), ("lisa", 59), ("tiago", 33), ("lilly", 36)],
            ["first_name", "age"],
        )
        assert_df_equality(res, expected)

    def it_handles_extra_columns():
        americans = spark.createDataFrame(
            [("bob", 42), ("lisa", 59)], ["first_name", "age"]
        )
        indians = spark.createDataFrame(
            [(55, "arjun", "cricket"), (5, "ira", "playing")],
            ["age", "first_name", "hobby"],
        )
        res = americans.unionByName(indians, allowMissingColumns=True)
        expected = spark.createDataFrame(
            [
                ("bob", 42, None),
                ("lisa", 59, None),
                ("arjun", 55, "cricket"),
                ("ira", 5, "playing"),
            ],
            ["first_name", "age", "hobby"],
        )
        assert_df_equality(res, expected)
