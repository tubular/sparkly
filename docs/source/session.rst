Sparkly Session
===============

``SparklySession`` is the main entry point to sparkly's functionality.
It's derived from ``SparkSession`` to provide additional features on top of the default session.
The are two main differences between ``SparkSession`` and ``SparklySession``:

    1. ``SparklySession`` doesn't have ``builder`` attribute,
       because we prefer declarative session definition over imperative.
    2. Hive support is enabled by default.

The example below shows both imperative and declarative approaches:

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


Installing dependencies
-----------------------

**Why**: Spark forces you to specify dependencies (spark packages or maven artifacts)
when a spark job is submitted (something like ``spark-submit --packages=...``).
We prefer a code-first approach where dependencies are actually
declared as part of the job.

**For example**: You want to read data from Cassandra.

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


Custom Maven repositories
-------------------------

**Why**: If you have a private maven repository, this is the way how to point spark to it to make package lookup.

**For example**: Let's assume your maven repository is available on: http://my.repo.net/maven,
and there is some spark package published there, with identifier: `my.corp:spark-handy-util:0.0.1`
You can install it to a spark session like this:

..code-block:: python

    from sparkly import SparklySession

    class MySession(SparklySession):
        repositories = ['http://my.repo.net/maven']
        packages = ['my.corp:spark-handy-util:0.0.1']

    spark = MySession()


Tuning options
--------------

**Why**: You want to customise your spark session.

**For example**:

    - ``spark.sql.shuffle.partitions`` to tune shuffling;
    - ``hive.metastore.uris`` to connect to your own HiveMetastore;
    - ``spark.hadoop.avro.mapred.ignore.inputs.without.extension`` package specific options.

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

**Why**: To start using Java UDF you have to import JAR file
via SQL query like ``add jar ../path/to/file`` and then call ``registerJavaFunction``.
We think it's too many actions for such simple functionality.

**For example**: You want to import UDFs from `brickhouse library <https://github.com/klout/brickhouse>`_.

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


API documentation
-----------------

.. automodule:: sparkly.session
    :members:
