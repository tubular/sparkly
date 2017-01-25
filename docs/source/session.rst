Sparkly Session
===============

`SparklySession` is the main entry point to `sparkly` functionality.
It's derived from `SparkSession` to provide additional features on top of the default session.

The are two main difference between `SparkSession` and `SparklySession`:

1) Unlike `SparkSession`, `SparklySession` doesn't have `builder` attribute.
We prefer declarative style over imperative.

.. code-block:: python

    # PySpark-style (imperative)
    from pyspark import SparkSession

    spark = SparkSession.builder\
        .appName('My App')\
        .master('spark://')\
        .config('spark.sql.shuffle.partitions', 10)\
        .getOrCreate()

    # Sparkly-style (declarative)
    from sparkly import SparklySession

    class MySession(SparklySession):
        options = {
            'spark.app.name': 'My App',
            'spark.master': 'spark://',
            'spark.sql.shuffle.partitions': 10,
        }

    spark = MySession()

    # In case you want to change default options
    spark = MySession({'spark.app.name': 'My Awesome App'})


2) Hive support is enabled by default.


Use cases
---------

Installing dependencies
^^^^^^^^^^^^^^^^^^^^^^^

**Why**: Spark forces you to specify dependencies (spark packages or maven artifacts)
when a spark job is submitted (something like `spark-submit --packages=...`).
We prefer a code-first approach where dependencies are actually
declared as part of the job.

**For example**: You want to use cassandra connector to read data from your cluster.

.. code-block:: python

    from sparkly import SparklySession


    class MySession(SparklySession):
        # Define a list of spark packages or maven artifacts.
        packages = [
            'datastax:spark-cassandra-connector:2.0.0-M2-s_2.11',
        ]

    # Dependencies will be fetched during the session initialisation.
    spark = MySession()

    # Here is how you now can access a dataset in Cassandra.
    df = spark.read_ext.by_url('cassandra://<cassandra-host>/<db>/<table>?consistency=QUORUM')


Tuning options
^^^^^^^^^^^^^^

**Why**: You want to customise your spark session.
**For example**:
    - `spark.sql.shuffle.partitions` to tune shuffling;
    - `hive.metastore.uris` to connect to your own HiveMetastore;
    - `spark.hadoop.avro.mapred.ignore.inputs.without.extension` package specific options.

.. code-block:: python

    from sparkly import SparklySession


    class MySession(SparklySession):
        options = {
            # Increase the default amount of partitions for shuffling.
            'spark.sql.shuffle.partitions': 1000,
            # Setup remote Hive Metastore.
            'hive.metastore.uris': 'thrift://<host1>:9083,thrift://<host2>:9083',
            # Ignore files without `avro` extensions.
            'spark.hadoop.avro.mapred.ignore.inputs.without.extension': 'false',
        }

    # You can also overwrite or add some options at initialisation time.
    spark = MySession({'spark.sql.shuffle.partitions': 10})


Using UDFs
----------

**Why**: By default to use UDFs in Hive queries you need to add jars and specify which
UDFs you wish to use using verbose Hive queries.

**For example**: You want to import UDFs from (brickhouse)[https://github.com/klout/brickhouse] Hive UDFs lib.

.. code-block:: python

    from pyspark.sql.types import IntegerType
    from sparkly import SparklySession


    def my_own_udf(item):
        return len(item)


    class MySession(SparklySession):
        # Import local jar files.
        jars = [
            '/path/to/brickhouse.jar'
        ]
        # Define UDFs.
        udfs = {
            'collect_max': 'brickhouse.udf.collect.CollectMaxUDAF',  # Java UDF.
            'my_udf': (my_own_udf, IntegerType()),  # Python UDF.
        }

    spark = MySession()

    spark.sql('SELECT collect_max(amount) FROM my_data GROUP BY ...')
    spark.sql('SELECT my_udf(amount) FROM my_data')


.. automodule:: sparkly.context
    :members:
