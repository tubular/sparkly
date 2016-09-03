import os

from pyspark import SparkConf, SparkContext, HiveContext
from sparkle.utils import absolute_path


class SparkleContext(HiveContext):
    """Wrapper to simplify packages, jars & options definition.

    There are several attributes that could be redefined in subclasses:
     - packages: list of spark packages to be installed (see. https://spark-packages.org/)
     - jars: list of full path to jars to be available in HiveContext
     - udfs: dictionary of udf functions to be available in context:
        - list of functions from jar:
            key=<function name>, value=<full class path>
        - list of user defined functions:
            key=<function name>, value=(<function def.>, <return type>)
     - options: dict of additional options to be passed to HiveContext
    """

    packages = []
    jars = []
    _default_jars = [
        absolute_path(__file__, 'resources', 'mysql-connector-java-5.1.39-bin.jar'),
        absolute_path(__file__, 'resources', 'brickhouse-0.7.1.jar'),
    ]
    options = {}
    udfs = {}

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

        for name, defn in self.udfs.items():
            if isinstance(defn, str):
                self.sql('create temporary function {} as "{}"'.format(name, defn))
            elif isinstance(defn, tuple):
                self.registerFunction(name, *defn)
            else:
                raise NotImplemented('Incorrect udf definition: {}: {}'.format(name, defn))
