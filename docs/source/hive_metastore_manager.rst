Hive Query Language Utils
=========================

.. automodule:: sparkle.hive_metastore_manager
    :members:

Hive metastore
^^^^^^^^^^^^^^

Hive metastore is a database storing metadata about Hive tables,
which you operate in your Sparkle (Hive) Context.
`Read more about Hive Metastore <http://www.cloudera.com/documentation/archive/cdh/4-x/4-2-0/CDH4-Installation-Guide/cdh4ig_topic_18_4.html>`_

To configure a SparkleContext to work with your Hive Metastore, you have to set `hive.metastore.uris` option.
You can do this via hive-site.xml file in spark config ($SPARK_HOME/conf/hive-site.xml), like this:
```
<property>
  <name>hive.metastore.uris</name>
  <value>thrift://<n.n.n.n>:9083</value>
  <description>IP address (or fully-qualified domain name) and port of the metastore host</description>
</property>
```

or set it dynamically in SparkleContext options, like this:

>>> class MySparkleContext(SparkleContext):
>>>    options = {
>>>        'hive.metastore.uris': 'thrift://<n.n.n.n>:9083',
>>>    }

After this your sparkle context will operate on the configured Hive Metastore.


Use cases
^^^^^^^^^

Check table existance
---------------------

*Why* sometimes logic of your program may depend on existance of the table in a Hive Metastore.
For example: to answer question if we should create a new table, or we need to replace an existing one.

>>> from sparkle import SparkleContext
>>> hc = SparkleContext()
>>> assert(hc.hms.table('my_table').exists() in {True, False})

Create table in hive metastore
------------------------------

*Why* You may want to unify access to all your data via Hive Metastore tables. To do this you generally need
to perform 'CREATE TABLE ...' statement for each data you have.

*Input* table name, data on some data storage hdfs or s3, stored in some specific format
(parquet, avro, csv, etc.)

*Output* table available in HiveMetastore

>>> from sparkle import SparkeContext
>>> # input
>>> hc = SparkleContext()
>>> df = hc.read_ext.by_url('parquet:s3://path/to/data/')
>>> # operation
>>> hc.hms.create_table(
>>>     'new_shiny_table',
>>>     df,
>>>     location='s3://path/to/data/',
>>>     partition_by=['partition', 'fields'],
>>>     output_format='parquet'
>>> )
>>> new_df = hc.read_ext.by_url('table://new_shiny_table')


Replace table in hive metastore
-------------------------------

*Why* some times you want to quickly replace data underlying some table in Hive Metastore.

*Input* table name to replace, data schema, location, partitioning, format.

*Output* updated table in Hive Metastore.

>>> from sparkle import SparkeContext
>>> # input
>>> hc = SparkleContext()
>>> df = hc.read_ext.by_url('csv:s3://path/to/data/new/')
>>> # operation
>>> table = hc.hms.replace_table(
>>>     'old_table',
>>>     df,
>>>     location='s3://path/to/data/',
>>>     partition_by=['partition', 'fields'],
>>> )


Operating on table properties
-----------------------------

*Why* some times you want to assign some metadata to your table like creation time, last update, purpose, source, etc.
Table properties is a perfect place for this.

Set/Get property

>>> form sparkle import SparkleContext
>>> hc = SparkleContext()
>>> table = hc.hms.table('my_table')
>>> table.set_property('foo', 'bar')
>>> assert(table.get_property('foo') == 'bar')
>>> table.get_all_properties() == {'foo': 'bar'}

*Note* properties may only have string keys and values, so you have to think on serialization from other
data types by yourself.