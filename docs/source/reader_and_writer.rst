.. _reader_and_writer:

Read/write utilities for DataFrames
===================================

Sparkly isn't trying to replace any of existing storage connectors.
The goal is to provide a simplified and consistent api across a wide array of storage connectors.
We also added the way to work with :ref:`abstract data sources <universal-reader-and-writer>`,
so you can keep your code agnostic to the storages you use.

.. _cassandra:

Cassandra
^^^^^^^^^

Sparkly relies on the official spark cassandra connector and was successfully tested in production using versions 1.5.x and 1.6.x.

+---------------+----------------------------------------------------------------------------------+
| Package       | https://spark-packages.org/package/datastax/spark-cassandra-connector            |
+---------------+----------------------------------------------------------------------------------+
| Configuration | https://github.com/datastax/spark-cassandra-connector/blob/b1.6/doc/reference.md |
+---------------+----------------------------------------------------------------------------------+

.. code-block:: python

    from sparkly import SparklyContext


    class MyContext(SparklyContext):
        # Feel free to play with other versions
        packages = ['datastax:spark-cassandra-connector:1.6.1-s_2.10']

    hc = MyContext()

    # To read data
    df = hc.read_ext.cassandra('localhost', 'my_keyspace', 'my_table')
    # To write data
    df.write_ext.cassandra('localhost', 'my_keyspace', 'my_table')

.. _csv:

CSV
^^^

Sparkly relies on the csv connector provided by `Databricks <databricks.com>`_.

.. note::

    Spark 2.x supports CSV out of the box.
    We highly recommend you to use `the official api <http://spark.apache.org/docs/2.0.0/api/python/pyspark.sql.html#pyspark.sql.DataFrameReader.csv>`_.

+---------------+---------------------------------------------------------+
| Package       | https://spark-packages.org/package/databricks/spark-csv |
+---------------+---------------------------------------------------------+
| Configuration | https://github.com/databricks/spark-csv#features        |
+---------------+---------------------------------------------------------+

.. code-block:: python

    from sparkly import SparklyContext


    class MyContext(SparklyContext):
        # Feel free to play with other versions
        packages = ['com.databricks:spark-csv_2.10:1.4.0']

    hc = MyContext()

    # To read data
    df = hc.read_ext.csv('/path/to/csv/file.csv', header=True)
    # To write data
    df.write_ext.csv('/path/to/csv/file.csv', header=False)

.. _elastic:

Elastic
^^^^^^^

Sparkly relies on the official elastic spark connector and was successfully tested in production using versions 2.2.x and 2.3.x.

+---------------+---------------------------------------------------------------------------------+
| Package       | https://spark-packages.org/package/elastic/elasticsearch-hadoop                 |
+---------------+---------------------------------------------------------------------------------+
| Configuration | https://www.elastic.co/guide/en/elasticsearch/hadoop/current/configuration.html |
+---------------+---------------------------------------------------------------------------------+

.. code-block:: python

    from sparkly import SparklyContext


    class MyContext(SparklyContext):
        # Feel free to play with other versions
        packages = ['org.elasticsearch:elasticsearch-spark_2.10:2.3.0']

    hc = MyContext()

    # To read data
    df = hc.read_ext.elastic('localhost', 'my_index', 'my_type', query='?q=awesomeness')
    # To write data
    df.write_ext.elastic('localhost', 'my_index', 'my_type')

.. _mysql:

MySQL
^^^^^

Basically, it's just a high level api on top of the native
`jdbc reader <http://spark.apache.org/docs/2.0.0/api/python/pyspark.sql.html#pyspark.sql.DataFrameReader.jdbc>`_ and
`jdbc writer <http://spark.apache.org/docs/2.0.0/api/python/pyspark.sql.html#pyspark.sql.DataFrameWriter.jdbc>`_.

+---------------+--------------------------------------------------------------------------------------------------+
| Jars          | https://dev.mysql.com/downloads/connector/j/                                                     |
+---------------+--------------------------------------------------------------------------------------------------+
| Configuration | https://dev.mysql.com/doc/connector-j/5.1/en/connector-j-reference-configuration-properties.html |
+---------------+--------------------------------------------------------------------------------------------------+

.. note::

    Sparkly doesn't contain any jars inside, so you will have to take care of this.
    Java connectors for mysql could be found on https://dev.mysql.com/downloads/connector/j/.
    We usually place them within our service/package codebase in `resources` directory.
    It's not the best idea to place binaries within a source code, but it's pretty convenient.

.. code-block:: python

    from sparkly import SparklyContext
    from sparkly.utils import absolute_path


    class MyContext(SparklyContext):
        # Feel free to play with other versions.
        jars = [absolute_path(__file__, './path/to/mysql-connector-java-5.1.39-bin.jar')]

    hc = MyContext()

    # To read data
    df = hc.read_ext.mysql('localhost', 'my_database', 'my_table',
                           options={'user': 'root', 'password': 'root'})
    # To write data
    df.write_ext.mysql('localhost', 'my_database', 'my_table', options={
        'user': 'root',
        'password': 'root',
        'rewriteBatchedStatements': 'true',  # improves write throughput dramatically
    })


.. _universal-reader-and-writer:

Universal reader/writer
^^^^^^^^^^^^^^^^^^^^^^^

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
^^^^^^^^^^^^^^^^^^^^

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
^^^^^^^^^^^^^^^^^^^^^^^^

.. automodule:: sparkly.reader
    :members:

Writer API documentation
^^^^^^^^^^^^^^^^^^^^^^^^

.. automodule:: sparkly.writer
    :members:
