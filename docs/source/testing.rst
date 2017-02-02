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
