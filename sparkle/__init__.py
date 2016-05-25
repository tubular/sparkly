import os

from pyspark import SparkConf, SparkContext, HiveContext
from sparkle.utils import absolute_path


class SparkleContext(HiveContext):
    """Wrapper to simplify packages, jars & options definition."""

    packages = []
    jars = []
    _default_jars = [
        absolute_path(__file__, 'resources', 'mysql-connector-java-5.1.39-bin.jar'),
    ]
    options = {}

    def __init__(self, additional_options=None):
        packages_args = ''
        if self.packages:
            packages_args = '--packages {}'.format(','.join(self.packages))

        jars_args = '--jars {}'.format(','.join(self._default_jars + self.jars))

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
