.. _reader_and_writer:

Read/write utilities for DataFrames
===================================

Sparkly isn't trying to replace any of existing storage connectors.
The goal is to provide a simplified and consistent api across a wide array of storage connectors.
We also added the way to work with :ref:`abstract data sources <universal-reader-and-writer>`,
so you can keep your code agnostic to the storages you use.

.. _cassandra:

Cassandra
---------

Sparkly relies on the official spark cassandra connector and was successfully tested in production using version `2.0.0-M2`.

+---------------+---------------------------------------------------------------------------------------+
| Package       | https://spark-packages.org/package/datastax/spark-cassandra-connector                 |
+---------------+---------------------------------------------------------------------------------------+
| Configuration | https://github.com/datastax/spark-cassandra-connector/blob/v2.0.0-M2/doc/reference.md |
+---------------+---------------------------------------------------------------------------------------+

.. code-block:: python

    from sparkly import SparklySession


    class MySession(SparklySession):
        # Feel free to play with other versions
        packages = ['datastax:spark-cassandra-connector:2.0.0-M2-s_2.11']

    spark = MySession()

    # To read data
    df = spark.read_ext.cassandra('localhost', 'my_keyspace', 'my_table')
    # To write data
    df.write_ext.cassandra('localhost', 'my_keyspace', 'my_table')


.. _elastic:

Elastic
-------

Sparkly relies on the official elastic spark connector and was successfully tested in production using version `5.1.1`.

+---------------+-----------------------------------------------------------------------------+
| Package       | https://spark-packages.org/package/elastic/elasticsearch-hadoop             |
+---------------+-----------------------------------------------------------------------------+
| Configuration | https://www.elastic.co/guide/en/elasticsearch/hadoop/5.1/configuration.html |
+---------------+-----------------------------------------------------------------------------+

.. code-block:: python

    from sparkly import SparklySession


    class MySession(SparklySession):
        # Feel free to play with other versions
        packages = ['org.elasticsearch:elasticsearch-spark-20_2.11:5.1.1']

    spark = MySession()

    # To read data
    df = spark.read_ext.elastic('localhost', 'my_index', 'my_type', query='?q=awesomeness')
    # To write data
    df.write_ext.elastic('localhost', 'my_index', 'my_type')

.. _kafka:

Kafka
-----

Sparkly's reader and writer for Kafka are built on top of the official spark package
for Kafka and python library `kafka-python <https://github.com/dpkp/kafka-python>`_ .
The first one allows us to read data efficiently,
the second covers a lack of writing functionality in the official distribution.

+---------------+------------------------------------------------------------------------------------------+
| Package       | https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka-0-8_2.11/2.1.0 |
+---------------+------------------------------------------------------------------------------------------+
| Configuration | http://spark.apache.org/docs/2.1.0/streaming-kafka-0-8-integration.html                  |
+---------------+------------------------------------------------------------------------------------------+

.. note::
    - To interact with Kafka, ``sparkly`` needs the ``kafka-python`` library. You can get it via:
      ```
      pip install sparkly[kafka]
      ```
    - Sparkly was tested in production using Apache Kafka **0.10.x**.

.. code-block:: python

    import json

    from sparkly import SparklySession


    class MySession(SparklySession):
        packages = [
            'org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0',
        ]

    spark = MySession()

    # To read JSON messaged from Kafka into a dataframe:

    #   1. Define a schema of the messages you read.
    df_schema = StructType([
        StructField('key', StructType([
            StructField('id', StringType(), True)
        ])),
        StructField('value', StructType([
            StructField('name', StringType(), True),
            StructField('surname', StringType(), True),
        ]))
    ])

    #   2. Specify the schema as a reader parameter.
    df = hc.read_ext.kafka(
        'kafka.host',
        topic='my.topic',
        key_deserializer=lambda item: json.loads(item.decode('utf-8')),
        value_deserializer=lambda item: json.loads(item.decode('utf-8')),
        schema=df_schema,
    )

    # To write a dataframe to Kafka in JSON format:
    df.write_ext.kafka(
        'kafka.host',
        topic='my.topic',
        key_serializer=lambda item: json.dumps(item).encode('utf-8'),
        value_serializer=lambda item: json.dumps(item).encode('utf-8'),
    )

.. _mysql:

MySQL
-----

Basically, it's just a high level api on top of the native
`jdbc reader <http://spark.apache.org/docs/2.1.0/api/python/pyspark.sql.html#pyspark.sql.DataFrameReader.jdbc>`_ and
`jdbc writer <http://spark.apache.org/docs/2.1.0/api/python/pyspark.sql.html#pyspark.sql.DataFrameWriter.jdbc>`_.

+---------------+--------------------------------------------------------------------------------------------------+
| Jars          | https://mvnrepository.com/artifact/mysql/mysql-connector-java                                    |
+---------------+--------------------------------------------------------------------------------------------------+
| Configuration | https://dev.mysql.com/doc/connector-j/5.1/en/connector-j-reference-configuration-properties.html |
+---------------+--------------------------------------------------------------------------------------------------+

.. code-block:: python

    from sparkly import SparklySession
    from sparkly.utils import absolute_path


    class MySession(SparklySession):
        # Feel free to play with other versions.
        packages = ['mysql:mysql-connector-java:5.1.39']


    spark = MySession()

    # To read data
    df = spark.read_ext.mysql('localhost', 'my_database', 'my_table',
                              options={'user': 'root', 'password': 'root'})
    # To write data
    df.write_ext.mysql('localhost', 'my_database', 'my_table', options={
        'user': 'root',
        'password': 'root',
        'rewriteBatchedStatements': 'true',  # improves write throughput dramatically
    })

.. _redis:

Redis
-----

Sparkly provides a writer for Redis that is built on top of the official redis python library
`redis-py <https://github.com/andymccurdy/redis-py>`_ .
It is currently capable of exporting your DataFrame as a JSON blob per row or group of rows.

.. note::
    - To interact with Redis, ``sparkly`` needs the ``redis`` library. You can get it via:
      ``pip install sparkly[redis]``

.. code-block:: python

    import json

    from sparkly import SparklySession


    spark = SparklySession()

    # Write JSON.gz data indexed by col1.col2 that will expire in a day
    df.write_ext.redis(
        host='localhost',
        port=6379,
        key_by=['col1', 'col2'],
        exclude_key_columns=True,
        expire=24 * 60 * 60,
        compression='gzip',
    )


.. _universal-reader-and-writer:

Universal reader/writer
-----------------------

The `DataFrame` abstraction is really powerful when it comes to transformations.
You can shape your data from various storages using exactly the same api.
For instance, you can join data from Cassandra with data from Elasticsearch and write the result to MySQL.

The only problem - you have to explicitly define sources (or destinations) in order to create (or export) a `DataFrame`.
But the source/destination of data doesn't really change the logic of transformations (if the schema is preserved).
To solve the problem, we decided to add the universal api to read/write `DataFrames`:

.. code-block:: python

    from sparkly import SparklyContext

    class MyContext(SparklyContext):
        packages = [
            'datastax:spark-cassandra-connector:1.6.1-s_2.10',
            'com.databricks:spark-csv_2.10:1.4.0',
            'org.elasticsearch:elasticsearch-spark_2.10:2.3.0',
        ]

    hc = MyContext()

    # To read data
    df = hc.read_ext.by_url('cassandra://localhost/my_keyspace/my_table?consistency=ONE')
    df = hc.read_ext.by_url('csv:s3://my-bucket/my-data?header=true')
    df = hc.read_ext.by_url('elastic://localhost/my_index/my_type?q=awesomeness')
    df = hc.read_ext.by_url('parquet:hdfs://my.name.node/path/on/hdfs')

    # To write data
    df.write_ext.by_url('cassandra://localhost/my_keyspace/my_table?consistency=QUORUM&parallelism=8')
    df.write_ext.by_url('csv:hdfs://my.name.node/path/on/hdfs')
    df.write_ext.by_url('elastic://localhost/my_index/my_type?parallelism=4')
    df.write_ext.by_url('parquet:s3://my-bucket/my-data?header=false')


.. _controlling-the-load:

Controlling the load
--------------------

From the official documentation:

    | Don’t create too many partitions in parallel on a large cluster; otherwise Spark might crash your external database systems.

    link: <https://spark.apache.org/docs/2.0.1/api/java/org/apache/spark/sql/DataFrameReader.html>

It's a very good advice, but in practice it's hard to track the number of partitions.
For instance, if you write a result of a join operation to database the number of splits
might be changed implicitly via `spark.sql.shuffle.partitions`.

To prevent us from shooting to the foot, we decided to add `parallelism` option for all our readers and writers.
The option is designed to control a load on a source we write to / read from.
It's especially useful when you are working with data storages like Cassandra, MySQL or Elastic.
However, the implementation of the throttling has some drawbacks and you should be aware of them.

The way we implemented it is pretty simple: we use `coalesce` on a dataframe
to reduce an amount of tasks that will be executed in parallel.
Let's say you have a dataframe with 1000 splits and you want to write no more than 10 task
in parallel. In such case `coalesce` will create a dataframe that has 10 splits
with 100 original tasks in each. An outcome of this: if any of these 100 tasks fails,
we have to retry the whole pack in 100 tasks.

`Read more about coalesce <http://spark.apache.org/docs/latest/programming-guide.html#CoalesceLink>`_

Reader API documentation
------------------------

.. automodule:: sparkly.reader
    :members:

Writer API documentation
------------------------

.. automodule:: sparkly.writer
    :members:
