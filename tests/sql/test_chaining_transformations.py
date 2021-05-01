import pytest
from spark import *

from pyspark.sql.functions import col, lit, concat


class TestChainingTransformations(object):
    def test_funify(self):
        def funify(col_name, str):
            def _(df):
                return df.withColumn(
                    "funified", concat(col(col_name), lit(" "), lit(str))
                )

            return _

        data = [("colombia",), ("brasil",), (None,)]
        df = spark.createDataFrame(data, ["country"])
        df.show(truncate=100)
        df.transform(funify("country", "is super fun!")).show(truncate=False)
