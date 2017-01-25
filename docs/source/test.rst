Integration Testing Base Classes
================================

Base testing classes
--------------------

There are two main testing classes in Sparkly:
 - SparklyTest:
    * Instantiates Sparkly session specified in `session` attribute.
    * The session instance will be available via `self.spark`.
 - SparklyGlobalContextTest:
    * Reuses a single SparklySession instance for all test cases to boost performance.

**Example:**

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
A fixture will load data to a database before the test execution.

There are several databases supported in Sparkly:
 - Mysql (requires: `PyMySql`)
 - Elastic
 - Cassandra (requires: `cassandra-driver`)

**Example:**

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
