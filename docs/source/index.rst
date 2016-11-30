Welcome to sparkly's documentation!
===================================

Sparkly is a lib which makes usage of pyspark more convenient and consistent.

A brief tour on Sparkly features by example:

.. code-block:: python

   # The main thing and the entry point of the Sparkly lib is SparklyContext
   from sparkly import SparklyContext

   class CustomSparklyContext(SparklyContext):
      # Install custom spark packages instead of hacking with `spark-submit`:
      packages = ['com.databricks:spark-csv_2.10:1.4.0']

      # Install jars and import udfs from them as simple as:
      jars = ['/path/to/brickhouse-0.7.1.jar'],
      udfs = {
        'collect_max': 'brickhouse.udf.collect.CollectMaxUDAF',
      }


   ctx = CustomSparkContext()

   # Operate with easily interchangable URL-like data source definitions,
   # instead of untidy default interface:
   df = ctx.read_ext.by_url('mysql://<my-sql.host>/my_database/my_database')
   df.write_ext('parquet:s3://<my-bucket>/<path>/data?partition_by=<field_name1>,<field_name1>')

   # Operate with Hive Metastore with convenient python api,
   # instead of verbose Hive queries:
   ctx.hms.create_table(
      'my_custom_table',
      df,
      location='s3://<my-bucket>/<path>/data',
      partition_by=[<field_name1>,<field_name1>],
      output_format='parquet'
   )

   # Make integration testing more convenient with Fixtures and base test classes:
   # SparklyTest, SparklyGlobalContextTest, instead of implementing you own spark testing
   # mini frameworks:
   class TestMyShinySparkScript(SparklyTest):
      fixtures = [
         MysqlFixture('<my-testing-host>', '<test-user>', '<test-pass>', '/path/to/data.sql', '/path/to/clear.sql')
      ]

      def test_job_works_with_mysql(self):
         df = self.hc.read_ext('mysql://<my-testing-host>/<test-db>/<test-table>?user=<test-usre>&password=<test-password>')
         res_df = my_shiny_script(df)
         self.assertDataframeEqual(
            res_df,
            [('DataA', 'DataB', 'DataC')],
            ['fieldA', 'fieldB', 'fieldC'],
         )

.. toctree::
   :maxdepth: 2

   context
   reader_and_writer
   hive_metastore_manager
   schema_parser
   utils
   exceptions
   test

.. automodule:: sparkly
   :members:

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
