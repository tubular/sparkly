Integration Testing Base Classes
================================

Base testing classes
^^^^^^^^^^^^^^^^^^^^

There are two main testing classes in Sparkly:
 - SparklyTest:
    * Instantiates Sparkly context specified in `context` attribute.
    * The context will be available via `self.hc`.
 - SparklyGlobalContextTest:
    * Reuses single SparklyContext for all tests for performance boost.

**Example:**

.. code-block:: python

    from sparkly import SparklyContext
    from sparkly.test import SparklyTest

    class MyTestCase(SparklyTest):
        context = SparklyContext
        def test(self):
            df = self.hc.read_ext.by_url(...)
            self.assertDataFrameEqual(
                df, [('test_data', 1)], ['name', 'number']
            )

    ...

    class MyTestWithReusableContext(SparklyGlobalContextTest):
        context = SparklyContext
        def test(self):
            df = self.hc.read_ext.by_url(...)

    ...

Fixtures
^^^^^^^^

Fixtures is term borrowed from testing in Django framework.
A fixture will load data to a database upon text execution.

There are couple of databases supported in Sparkly:
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
