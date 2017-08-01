import unittest
from spark import *

class TestDataFrameMethods(unittest.TestCase):

    def test_dataframe_equality(self):
        people1 = [('Alice', 1)]
        p1 = spark.createDataFrame(people1)

        people2 = [('Alice', 1)]
        p2 = spark.createDataFrame(people2)

        self.assertEqual(p1.collect(), p2.collect())

    def test_count(self):
        df = spark.range(5)
        self.assertEqual(df.count(), 5)

if __name__ == '__main__':
    unittest.main()


