Sparkle Context
---------------

About Sparkle Context
^^^^^^^^^^^^^^^^^^^^^

``SparkleContext`` class is the main class of the Sparkle library. It encompasses all the lib functionality.
Most of times you want to subclass it to define your usecase needs in class attributes.

Sparkle context have links to other extras of the lib:

================ ================================
     Attribute    Link to the doc
================ ================================
``read_ext``     :ref:`reader_and_writer`
``hms``          :ref:`hive_metastore_manager`
================ ================================

``Dataframe`` pyspark class is also monkey patched with.
``write_ext`` (:ref:`reader_and_writer`) attribute for convenient writing.

Use cases
^^^^^^^^^

Setup Custom options
--------------------

**Why**: Sometimes you need to customize your spark context more than default.
We like it defined in declarative way with not boilerplate with setters/getters.

**For example**: some useful usecases of this are:

    - Optimizing shuffling options, like ``spark.sql.shuffle.partitions``
    - Setup custom Hive Metastore instead of local.
    - Package specific options, like ``spark.hadoop.avro.mapred.ignore.inputs.without.extension``

.. code-block:: python

    from sparkle import SparkleContext
    class OwnSparkleContext(SparkleContext):
        options = {
            # Increasing default amount of partitions for shuffling.
            'spark.sql.shuffle.partitions': 1000,
            # setup remote Hive Metastore.
            'hive.metastore.uris': 'thrift://<host1>:9083,thrift://<host2>:9083',
            # setup avro reader to not ignore files without `avro` extension
            'spark.hadoop.avro.mapred.ignore.inputs.without.extension': 'false',
        }

    # you can also overwrite or add some options at initialisation time.
    cnx = OwnSparkleContext({ ...initialize-time options... })

    # you still can update options later if you need.
    cnx.setConf('key', 'value')

Installing spark dependencies
-----------------------------

**Why**: default mechanism for this is quite uncomfortable `spark-submit`.

**For example**: You want to install cassandra connector to read data for one of
your tables.

.. code-block:: python

    from sparkle import SparkleContext
    class OwnSparkleContext(SparkleContext):
        # specifying spark dependencies.
        packages = [
            'datastax:spark-cassandra-connector:1.5.0-M3-s_2.10',
        ]

    # dependencies will be installed in context initialization.
    cnx = OwnSparkleContext()

    # Here is how you now can obtain a Dataframe representing yout cassandra table.
    df = cnx.read_ext.by_url('cassandra://<cassandra-host>'
                             '/<db>/<talbe>?consistency=QUORUM&parallelism=16')


Using UDFs
----------

**Why**: By default to use udfs in Hive queries you need to add jars and specify which
ufds you wish to use using verbose Hive queries.

**For example**: You want to import udfs from (brickhouse)[https://github.com/klout/brickhouse] Hive udfs lib.

.. code-block:: python

    from pyspark.sql.types import IntegerType
    from sparkle import SparkleContext

    def my_own_udf(item):
        return len(item)

    class OwnSparkleContext(SparkleContext):
        # specifying spark dependencies.
        jars = [
            '/path/to/brickhouse.jar'
        ]
        udfs = {
            'collect_max': 'brickhouse.udf.collect.CollectMaxUDAF',
            'my_udf': (my_own_udf, IntegerType())
        }

    # dependencies will be installed in context initialization.
    cnx = OwnSparkleContext()

    cnx.sql('SELECT collect_max(amount) FROM my_data GROUP BY ...')
    cnx.sql('SELECT my_udf(amount) FROM my_data')


.. automodule:: sparkle.context
    :members:
