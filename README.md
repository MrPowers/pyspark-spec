# pyspark-spec

A test suite to document the behavior of the Pyspark engine.

## How to run a test file

* Download Spark
* Run a test file: `spark_path/bin/spark-submit sql/dataframe.py`

## How to supress INFO logs

Open the `conf/log4j.properties` file and change the logging level to ERROR: `log4j.rootCategory=ERROR, console`.  [Detailed instructions here](https://stackoverflow.com/questions/27781187/how-to-stop-messages-displaying-on-spark-console).
