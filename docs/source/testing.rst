Testing Utils
=============

Base TestCases
--------------

There are two main test cases available in Sparkly:
 - ``SparklyTest`` creates a new session for each test case.
 - ``SparklyGlobalSessionTest`` uses a single sparkly session for all test cases to boost performance.

.. code-block:: python

    from pyspark.sql import types as T

    from sparkly import SparklySession
    from sparkly.test import SparklyTest


    class MyTestCase(SparklyTest):
        session = SparklySession

        def test(self):
            df = self.spark.read_ext.by_url(...)

            # Compare all fields
            self.assertRowsEqual(
                df.collect(),
                [
                    T.Row(col1='row1', col2=1),
                    T.Row(col1='row2', col2=2),
                ],
            )

    ...

    class MyTestWithReusableSession(SparklyGlobalSessionTest):
        context = SparklySession

        def test(self):
            df = self.spark.read_ext.by_url(...)

    ...


DataFrame Assertions
--------------------

Asserting that the dataframe produced by your transformation is equal to some expected
output can be unnecessarily complicated at times. Common issues include:

- Ignoring the order in which elements appear in an array.
  This could be particularly useful when that array is generated as part of a
  ``groupBy`` aggregation, and you only care about all elements being part of the end
  result, rather than the order in which Spark encountered them.
- Comparing floats that could be arbitrarily nested in complicated datatypes
  within a given tolerance; exact matching is either fragile or impossible.
- Ignoring whether a field of a complex datatype is nullable.
  Spark infers this based on the applied transformations, but it is oftentimes
  inaccurate. As a result, assertions on complex data types might fail, even
  though in theory they shouldn't have.
- Having rows with different field names compare equal if the values match in
  alphabetical order of the names (see unit tests for example).
- Unhelpful diffs in case of mismatches.

Sparkly addresses these issues by providing ``assertRowsEqual``:

.. code-block:: python

    from pyspark.sql import types as T

    from sparkly import SparklySession
    from sparkly.test import SparklyTest


    def my_transformation(spark):
        return spark.createDataFrame(
            data=[
                ('row1', {'field': 'value_1'}, [1.1, 2.2, 3.3]),
                ('row2', {'field': 'value_2'}, [4.1, 5.2, 6.3]),
            ],
            schema=T.StructType([
                T.StructField('id', T.StringType()),
                T.StructField(
                    'st',
                    T.StructType([
                        T.StructField('field', T.StringType()),
                    ]),
                ),
                T.StructField('ar', T.ArrayType(T.FloatType())),
            ]),
        )


    class MyTestCase(SparklyTest):
        session = SparklySession

        def test(self):
            df = my_transformation(self.spark)

            self.assertRowsEqual(
                df.collect(),
                [
                    T.Row(id='row2', st=T.Row(field='value_2'), ar=[6.0, 5.0, 4.0]),
                    T.Row(id='row1', st=T.Row(field='value_1'), ar=[2.0, 3.0, 1.0]),
                ],
                atol=0.5,
            )


Instant Iterative Development
-----------------------------

The slowest part in Spark integration testing is context initialisation.
``SparklyGlobalSessionTest`` allows you to keep the same instance of spark context between different test cases,
but it still kills the context at the end. It's especially annoying if you work in `TDD fashion <https://en.wikipedia.org/wiki/Test-driven_development>`_.
On each run you have to wait 25-30 seconds till a new context is ready.
We added a tool to preserve spark context between multiple test runs.

.. code-block::

    # Activate instant testing mode.
    sparkly-testing up

    # The first run is slow (context is created).
    py.test tests/my_integration_test_with_sparkly.py

    # The second run and all after it are fast (context is reused).
    py.test tests/my_integration_test_with_sparkly.py

    # Deactivate instant testing mode (when you are done with testing).
    sparkly-testing down

.. note::
    In case if you change ``SparklySession`` definition (new options, jars or packages)
    you have to refresh the context via ``sparkly-testing refresh``.
    However, you don't need to refresh context if ``udfs`` are changed.


Fixtures
--------

"Fixture" is a term borrowed from Django framework.
Fixtures load data to a database before the test execution.

There are several storages supported in Sparkly:
    - Elastic
    - Cassandra (requires ``cassandra-driver``)
    - Mysql (requires ``PyMySql``)
    - Kafka (requires ``kafka-python``)

.. code-block:: python

    from sparkly.test import MysqlFixture, SparklyTest


    class MyTestCase(SparklyTest):
        ...
        fixtures = [
            MysqlFixture('mysql.host',
                         'user',
                         'password',
                         '/path/to/setup_data.sql',
                         '/path/to/remove_data.sql')
        ]
        ...

.. automodule:: sparkly.testing
    :members:
