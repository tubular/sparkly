Hive Metastore Utils
====================

About Hive Metastore
--------------------

The Hive Metastore is a database with metadata for Hive tables.

To configure ``SparklySession`` to work with external Hive Metastore, you need to set ``hive.metastore.uris`` option.
You can do this via ``hive-site.xml`` file in spark config ($SPARK_HOME/conf/hive-site.xml):

.. code-block:: xml

    <property>
      <name>hive.metastore.uris</name>
      <value>thrift://<n.n.n.n>:9083</value>
      <description>IP address (or fully-qualified domain name) and port of the metastore host</description>
    </property>


or set it dynamically via ``SparklySession`` options:

.. code-block:: python

    class MySession(SparklySession):
        options = {
            'hive.metastore.uris': 'thrift://<n.n.n.n>:9083',
        }


Tables management
-----------------

**Why:** you need to check if tables exist, rename them, drop them, or even overwrite existing aliases in your catalog.

.. code-block:: python

    from sparkly import SparklySession


    spark = SparklySession()

    assert spark.catalog_ext.has_table('my_table') in {True, False}
    spark.catalog_ext.rename_table('my_table', 'my_new_table')
    spark.catalog_ext.create_table('my_new_table', path='s3://my/parquet/data', source='parquet', mode='overwrite')
    spark.catalog_ext.drop_table('my_new_table')

Table properties management
---------------------------

**Why:** sometimes you want to assign custom attributes for your table, e.g. creation time, last update, purpose, data source.
The only way to interact with table properties in spark - use raw SQL queries.
We implemented a more convenient interface to make your code cleaner.

.. code-block:: python

    from sparkly import SparklySession


    spark = SparklySession()
    spark.catalog_ext.set_table_property('my_table', 'foo', 'bar')
    assert spark.catalog_ext.get_table_property('my_table', 'foo') == 'bar'
    assert spark.catalog_ext.get_table_properties('my_table') == {'foo': 'bar'}

*Note* properties are stored as strings.
In case if you need other types, consider using a serialisation format, e.g. JSON.


Using non-default database
--------------------------

**Why** to split your warehouse into logical groups (for example by system components).
In all catalog_ext.* methods you can specify full table names <db-name>.<table-name> and
it should operate properly

.. code-block:: python

    from time import time
    from sparkly import SparklySession

    spark = SparklySession()

    if spark.catalog_ext.has_database('my_database'):
        self.catalog_ext.rename_table(
            'my_database.my_badly_named_table',
            'new_shiny_name',
        )
        self.catalog_ext.set_table_property(
            'my_database.new_shiny_name',
            'last_update_at',
            time(),
        )

*Note* be careful using 'USE' statements like: spark.sql('USE my_database'),
it's stateful and may lead to weird errors, if code assumes correct current database.


API documentation
-----------------

.. automodule:: sparkly.catalog
    :members:
