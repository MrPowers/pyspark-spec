import pytest
from spark import *
import datetime

from pyspark.sql.functions import *

class TestFunctions(object):

    def test_year(self):
        sourceDF = spark.createDataFrame(
            [("jose", datetime.date(2017, 1, 1)), ("li", datetime.date(2015, 7, 6))], ["name", "birthdate"]
        )

        actualDF = sourceDF.withColumn("birthyear", year(col("birthdate")))

        expectedDF = spark.createDataFrame(
            [
                ("jose", datetime.date(2017, 1, 1), 2017),
                ("li", datetime.date(2015, 7, 6), 2015)
            ],
            [
                "name",
                "birthdate"
            ]
        )

        assert(expectedDF.collect() == actualDF.collect())
