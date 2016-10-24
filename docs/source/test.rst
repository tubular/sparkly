Integration Testing Base Classes
================================

Base testing classes
^^^^^^^^^^^^^^^^^^^^

There are two main testing classes in Sparkle:
 - SparkleTest:
    * Instantiates Sparkle context specified in `context` attribute.
    * The context will be available via `self.hc`.
 - SparkleGlobalContextTest:
    * Reuses single SparkleContext for all tests for performance boost.

**Example:**

.. code-block:: python

    from sparkle import SparkleContext
    from sparkle.test import SparkleTest

    class MyTestCase(SparkleTest):
        context = SparkleContext
        def test(self):
            df = self.hc.read_ext.by_url(...)
            self.assertDataframeEqual(
                df, [('test_data', 1)], ['name', 'number']
            )

    ...

    class MyTestWithReusableContext(SparkleGlobalContextTest):
        context = SparkleContext
        def test(self):
            df = self.hc.read_ext.by_url(...)

    ...

Fixtures
^^^^^^^^

Fixtures is term borrowed from testing in Django framework.
It's a data to be loaded to a database on test execution.

There are couple of databases supported in Sparkle:
 - Mysql
 - Elastic
 - Cassandra

**Example:**

.. code-block:: python

    from sparkle.test import MysqlFixture, SparkleTest

    class MyTestCase(SparkleTest):
        ...
        fixtures = [
            MysqlFixture('mysql.host',
                         'user',
                         'password',
                         '/path/to/setup_data.sql',
                         '/path/to/remove_data.sql')
        ]
        ...

.. automodule:: sparkle.test
    :members:
