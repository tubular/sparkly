Testing Utils
=============

Base TestCases
--------------

There are two main test cases available in Sparkly:
 - ``SparklyTest`` creates a new sparkly session per each test case.
 - ``SparklyGlobalSessionTest`` uses a single sparkly session for all test cases to boot performance.

.. code-block:: python

    from sparkly import SparklySession
    from sparkly.test import SparklyTest


    class MyTestCase(SparklyTest):
        session = SparklySession

        def test(self):
            df = self.spark.read_ext.by_url(...)
            self.assertDataFrameEqual(
                df, [('test_data', 1)], ['name', 'number']
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
