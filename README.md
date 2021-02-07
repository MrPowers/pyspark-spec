# pyspark-spec

A test suite to document the behavior of the PySpark engine.

## How to run the tests

This project uses Poetry for dependency management.

* Run `poetry install` to install all the project dependencies on a virtual environment
* Run the tests with `poetry run pytest tests`

## How to suppress INFO logs

Open the `conf/log4j.properties` file and change the logging level to ERROR: `log4j.rootCategory=ERROR, console`.  [Detailed instructions here](https://stackoverflow.com/questions/27781187/how-to-stop-messages-displaying-on-spark-console).
