import pytest
from spark import *

from chispa.dataframe_comparer import *
from pyspark.sql.functions import col


class TestDataFrame(object):
    def test_coalesce(self):
        pytest.skip("to be added")

    def test_collect(self):
        pytest.skip("to be added")

    def test_columns(self):
        data = [("jose", 1), ("li", 2), ("luisa", 3)]
        sourceDF = spark.createDataFrame(data, ["name", "age"])
        assert sourceDF.columns == ["name", "age"]

    def test_corr(self):
        data = [(1, 10), (2, 15), (3, 33)]
        sourceDF = spark.createDataFrame(data, ["quiz1", "quiz2"])
        corr = sourceDF.corr("quiz1", "quiz2")
        assert pytest.approx(0.95, 0.1) == corr

    def test_count(self):
        df = spark.range(5)
        assert df.count() == 5

    def test_cov(self):
        pytest.skip("to be added")

    def test_cross_join(self):
        pytest.skip("to be added")

    def test_cross_tab(self):
        pytest.skip("to be added")

    def test_cube(self):
        pytest.skip("to be added")

    def test_describe(self):
        pytest.skip("to be added")

    def test_distinct(self):
        source_data = [("jose", 1), ("li", 2), ("jose", 1)]
        sourceDF = spark.createDataFrame(source_data, ["name", "age"])

        actualDF = sourceDF.distinct()

        expected_data = [("jose", 1), ("li", 2)]
        expectedDF = spark.createDataFrame(expected_data, ["name", "age"])

        assert sorted(expectedDF.collect()) == sorted(actualDF.collect())

    def test_drop(self):
        pytest.skip("to be added")

    def test_drop_duplicates(self):
        source_data = [("jose", 1), ("li", 2), ("jose", 1)]
        sourceDF = spark.createDataFrame(source_data, ["name", "age"])

        actualDF = sourceDF.drop_duplicates()

        expected_data = [("jose", 1), ("li", 2)]
        expectedDF = spark.createDataFrame(expected_data, ["name", "age"])

        assert sorted(expectedDF.collect()) == sorted(actualDF.collect())

    def test_drop_na(self):
        pytest.skip("to be added")

    def test_dtypes(self):
        pytest.skip("to be added")

    def test_explain(self):
        pytest.skip("to be added")

    def test_fillna(self):
        pytest.skip("to be added")

    def test_filer(self):
        pytest.skip("to be added")

    def test_first(self):
        pytest.skip("to be added")

    def test_foreach(self):
        pytest.skip("to be added")

    def test_foreachPartition(self):
        pytest.skip("to be added")

    def test_groupby(self):
        pytest.skip("to be added")

    def test_head(self):
        pytest.skip("to be added")

    def test_intersect(self):
        pytest.skip("to be added")

    def test_join(self):
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

    def test_limit(self):
        pytest.skip("to be added")

    def test_na(self):
        pytest.skip("to be added")

    def test_select_with_array_argument(self):
        data = [("jose", 1, "mexico"), ("li", 2, "china"), ("sandy", 3, "usa")]
        source_df = spark.createDataFrame(data, ["name", "age", "country"])

        actual_df = source_df.select(["age", "name"])

        data = [(1, "jose"), (2, "li"), (3, "sandy")]
        expected_df = spark.createDataFrame(data, ["age", "name"])

        assert sorted(actual_df.collect()) == sorted(expected_df.collect())

    def test_select_with_multiple_string_arguments(self):
        data = [("jose", 1, "mexico"), ("li", 2, "china"), ("sandy", 3, "usa")]
        source_df = spark.createDataFrame(data, ["name", "age", "country"])
        actual_df = source_df.select("age", "name")
        data = [(1, "jose"), (2, "li"), (3, "sandy")]
        expected_df = spark.createDataFrame(data, ["age", "name"])
        assert sorted(actual_df.collect()) == sorted(expected_df.collect())


    def test_union(self):
        americans = spark.createDataFrame(
            [("bob", 42), ("lisa", 59)], ["first_name", "age"]
        )
        colombians = spark.createDataFrame(
            [("maria", 20), ("camilo", 31)], ["first_name", "age"]
        )
        res = americans.union(colombians)
        expected = spark.createDataFrame(
            [("bob", 42), ("lisa", 59), ("maria", 20), ("camilo", 31)], ["first_name", "age"]
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


    def test_unionByName(self):
        americans = spark.createDataFrame(
            [("bob", 42), ("lisa", 59)], ["first_name", "age"]
        )
        brasilans = spark.createDataFrame(
            [(33, "tiago"), (36, "lilly")], ["age", "first_name"]
        )
        res = americans.unionByName(brasilans)
        expected = spark.createDataFrame(
            [("bob", 42), ("lisa", 59), ("tiago", 33), ("lilly", 36)], ["first_name", "age"]
        )
        assert_df_equality(res, expected)


    def test_unionByName_with_option(self):
        americans = spark.createDataFrame(
            [("bob", 42), ("lisa", 59)], ["first_name", "age"]
        )
        indians = spark.createDataFrame(
            [(55, "arjun", "cricket"), (5, "ira", "playing")], ["age", "first_name", "hobby"]
        )
        res = americans.unionByName(indians, allowMissingColumns=True)
        expected = spark.createDataFrame(
            [("bob", 42, None), ("lisa", 59, None), ("arjun", 55, "cricket"), ("ira", 5, "playing")], ["first_name", "age", "hobby"]
        )
        assert_df_equality(res, expected)
