import os

from pyspark import SparkConf, SparkContext, HiveContext


class SparkleContext(HiveContext):
    """Wrapper to simplify packages, jars & options definition."""
    packages = []
    jars = []
    options = {}

    def __init__(self, additional_options=None):
        packages_args = ''
        if self.packages:
            packages_args = '--packages {}'.format(','.join(self.packages))

        jars_args = ''
        if self.jars:
            jars_args = '--jars {}'.format(','.join(self.jars))

        os.environ['PYSPARK_SUBMIT_ARGS'] = '{} {} pyspark-shell'.format(packages_args, jars_args)

        options = list(self.options.items())
        if additional_options:
            options += list(additional_options.items())

        spark_conf = SparkConf()
        spark_conf.setAll(options)

        sc = SparkContext(conf=spark_conf)

        if os.environ.get('SPARK_TESTING'):
            super(SparkleContext, self).__init__(
                sc, sc._jvm.org.apache.spark.sql.hive.test.TestHiveContext(sc._jsc.sc()))
        else:
            super(SparkleContext, self).__init__(sc)
