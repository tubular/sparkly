Testing Utils
=============

Base TestCases
--------------

There are two main test cases available in Sparkly:
 - ``SparklyTest`` creates a new session for each test case.
 - ``SparklyGlobalSessionTest`` uses a single sparkly session for all test cases to boost performance.

.. code-block:: python

    from sparkly import SparklySession
    from sparkly.test import SparklyTest


    class MyTestCase(SparklyTest):
        session = SparklySession

        def test(self):
            df = self.spark.read_ext.by_url(...)

            # Compare all fields
            self.assertDataFrameEqual(
                actual_df=df,
                expected_data=[
                    {'col1': 'row1', 'col2': 1},
                    {'col1': 'row2', 'col2': 2},
                ],
            )

            # Compare a subset of fields
            self.assertDataFrameEqual(
                actual_df=df,
                expected_data=[
                    {'col1': 'row1'},
                    {'col1': 'row2'},
                ],
                fields=['col1'],
            )

    ...

    class MyTestWithReusableSession(SparklyGlobalSessionTest):
        context = SparklySession

        def test(self):
            df = self.spark.read_ext.by_url(...)

    ...

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
    _ Kafka (requires ``kafka-python``)

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
