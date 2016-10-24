Hive Query Language Utils
=========================

About Hive Metastore
^^^^^^^^^^^^^^^^^^^^

Hive metastore is a database storing metadata about Hive tables,
which you operate in your Sparkle (Hive) Context.
`Read more about Hive Metastore <http://www.cloudera.com/documentation/archive/cdh/4-x/4-2-0/CDH4-Installation-Guide/cdh4ig_topic_18_4.html>`_

To configure a SparkleContext to work with your Hive Metastore, you have to set `hive.metastore.uris` option.
You can do this via hive-site.xml file in spark config ($SPARK_HOME/conf/hive-site.xml), like this:

.. code-block:: xml

    <property>
      <name>hive.metastore.uris</name>
      <value>thrift://<n.n.n.n>:9083</value>
      <description>IP address (or fully-qualified domain name) and port of the metastore host</description>
    </property>


or set it dynamically in SparkleContext options, like this:

.. code-block:: python

    class MySparkleContext(SparkleContext):
        options = {
            'hive.metastore.uris': 'thrift://<n.n.n.n>:9083',
        }

After this your sparkle context will operate on the configured Hive Metastore.


Use cases
^^^^^^^^^

Check for existance
-------------------

**Why:** sometimes logic of your program may depend on existance of a table in a Hive Metastore.
**For example**: to know if we should create a new table, or we need to replace an existing one.


.. code-block:: python

    from sparkle import SparkleContext
    hc = SparkleContext()
    assert(hc.hms.table('my_table').exists() in {True, False})

.. _create_table:

Create a table in hive metastore
--------------------------------

**Why:** You may want to unify access to all your data via Hive Metastore tables. To do this you generally need
to perform 'CREATE TABLE ...' statement for each data you have. To simplify this we implemented this method which
generates the CREATE TABLE statements by passed parameters and executes them on Hive Metastore.

**Input:** table name, data on some data storage hdfs or s3, stored in some specific format
(parquet, avro, csv, etc.)

**Output:** table available in HiveMetastore

.. code-block:: python

    from sparkle import SparkeContext
    # input
    hc = SparkleContext()
    df = hc.read_ext.by_url('parquet:s3://path/to/data/')
    # operation
    hc.hms.create_table(
         'new_shiny_table',
         df,
         location='s3://path/to/data/',
         partition_by=['partition', 'fields'],
         output_format='parquet'
    )
    new_df = hc.read_ext.by_url('table://new_shiny_table')

.. _replace_table:
Replace table in hive metastore
-------------------------------

**Why:** some times you want to quickly replace data underlying some table in Hive Metastore.
For example, if you exported a new snapshot of your data to a new location and want to point
Hive Metastore table to this new location. This method avoids downtime during which data in the
table won't be accessible. It first creates a new table separately (slow operation) and
then operating on meta data (quick renaming operation).

***Input:*** table name to replace, data schema, location, partitioning, format.

**Output:** updated table in Hive Metastore.

.. code-block:: python

    from sparkle import SparkeContext
    # input
    hc = SparkleContext()
    df = hc.read_ext.by_url('csv:s3://path/to/data/new/')
    # operation
    table = hc.hms.replace_table(
        'old_table',
        df,
        location='s3://path/to/data/',
        partition_by=['partition', 'fields'],
    )


.. _table_properties:
Operating on table properties
-----------------------------

**Why:** some times you want to assign some metadata to your table like creation time, last update, purpose, data source, etc.
Table properties is a perfect place for this. Generally you have to execute Sql queries and parse results to manipulate
table properties. We implemented a more convenient interface on top of this.

**Set/Get property**

.. code-block:: python

    from sparkle import SparkleContext
    hc = SparkleContext()
    table = hc.hms.table('my_table')
    table.set_property('foo', 'bar')
    assert table.get_property('foo') == 'bar'
    assert table.get_all_properties() == {'foo': 'bar'}

*Note* properties may only have string keys and values, so you have to think on serialization from other
data types by yourself.


API Documentation
^^^^^^^^^^^^^^^^^

.. automodule:: sparkle.hive_metastore_manager
    :members:
