Welcome to sparkly's documentation!
===================================

Sparkly is a lib that makes usage of pyspark more convenient and consistent.

A brief tour on Sparkly features:

.. code-block:: python

   # The main entry point is SparklySession,
   # you can think of it as of a combination of SparkSession and SparkSession.builder.
   from sparkly import SparklySession


   # Define dependencies in the code instead of messing with `spark-submit`.
   class MySession(SparklySession):
       # Spark packages and dependencies from Maven.
       packages = [
           'datastax:spark-cassandra-connector:2.0.0-M2-s_2.11',
           'mysql:mysql-connector-java:5.1.39',
       ]

       # Jars and Hive UDFs
       jars = ['/path/to/brickhouse-0.7.1.jar'],
       udfs = {
           'collect_max': 'brickhouse.udf.collect.CollectMaxUDAF',
       }


   spark = MySession()

   # Operate with interchangeable URL-like data source definitions:
   df = spark.read_ext.by_url('mysql://<my-sql.host>/my_database/my_database')
   df.write_ext('parquet:s3://<my-bucket>/<path>/data?partition_by=<field_name1>')

   # Interact with Hive Metastore via convenient python api,
   # instead of verbose SQL queries:
   spark.catalog_ext.has_table('my_custom_table')
   spark.catalog_ext.get_table_properties('my_custom_table')

   # Easy integration testing with Fixtures and base test classes.
   from sparkly.testing import SparklyTest


   class TestMyShinySparkScript(SparklyTest):
       session = MySession

       fixtures = [
           MysqlFixture('<my-testing-host>', '<test-user>', '<test-pass>', '/path/to/data.sql', '/path/to/clear.sql')
       ]

      def test_job_works_with_mysql(self):
         df = self.spark.read_ext.by_url('mysql://<my-testing-host>/<test-db>/<test-table>?user=<test-usre>&password=<test-password>')
         res_df = my_shiny_script(df)
         self.assertDataFrameEqual(
            res_df,
            {'fieldA': 'DataA', 'fieldB': 'DataB', 'fieldC': 'DataC'},
         )

.. toctree::
   :maxdepth: 2

   session
   reader_and_writer
   catalog
   testing
   utils
   license

.. automodule:: sparkly
   :members:

Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
