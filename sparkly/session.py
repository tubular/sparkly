#
# Copyright 2017 Tubular Labs, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os
import signal
import sys

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

from sparkly.catalog import SparklyCatalog
from sparkly.instant_testing import InstantTesting
from sparkly.reader import SparklyReader
from sparkly.writer import attach_writer_to_dataframe


class SparklySession(SparkSession):
    """Wrapper around HiveContext to simplify definition of options, packages, JARs and UDFs.

    Example::

        from pyspark.sql.types import IntegerType
        import sparkly


        class MySession(sparkly.SparklySession):
            options = {'spark.sql.shuffle.partitions': '2000'}
            repositories = ['http://packages.confluent.io/maven/']
            packages = ['com.databricks:spark-csv_2.10:1.4.0']
            jars = ['../path/to/brickhouse-0.7.1.jar']
            udfs = {
                'collect_max': 'brickhouse.udf.collect.CollectMaxUDAF',
                'my_python_udf': (lambda x: len(x), IntegerType()),
            }


        spark = MySession()
        spark.read_ext.cassandra(...)

    Attributes:
        options (dict[str,str]): Configuration options that are passed to SparkConf.
            See `the list of possible options
            <https://spark.apache.org/docs/2.1.0/configuration.html#available-properties>`_.
        repositories (list[str]): List of additional maven repositories for package lookup.
        packages (list[str]): Spark packages that should be installed.
            See https://spark-packages.org/
        jars (list[str]): Full paths to jar files that we want to include to the session.
            E.g. a JDBC connector or a library with UDF functions.
        udfs (dict[str,str|typing.Callable]): Register UDF functions within the session.
            Key - a name of the function,
            Value - either a class name imported from a JAR file
                or a tuple with python function and its return type.
    """
    options = {}
    packages = []
    jars = []
    udfs = {}
    repositories = []

    def __init__(self, additional_options=None):
        os.environ['PYSPARK_PYTHON'] = sys.executable
        submit_args = [
            self._setup_repositories(),
            self._setup_packages(),
            self._setup_jars(),
            'pyspark-shell',
        ]
        os.environ['PYSPARK_SUBMIT_ARGS'] = ' '.join(filter(None, submit_args))

        def _create_spark_context():
            spark_conf = SparkConf()
            spark_conf.set('spark.sql.catalogImplementation', 'hive')
            spark_conf.setAll(self._setup_options(additional_options))
            return SparkContext(conf=spark_conf)

        # If we are in instant testing mode
        if InstantTesting.is_activated():
            spark_context = InstantTesting.get_context()

            # It's the first run, so we have to create context and demonise the process.
            if spark_context is None:
                spark_context = _create_spark_context()
                if os.fork() == 0:  # Detached process.
                    signal.pause()
                else:
                    InstantTesting.set_context(spark_context)
        else:
            spark_context = _create_spark_context()

        # Init HiveContext
        super(SparklySession, self).__init__(spark_context)
        self._setup_udfs()

        self.read_ext = SparklyReader(self)
        self.catalog_ext = SparklyCatalog(self)

        attach_writer_to_dataframe()

    @property
    def builder(self):
        raise NotImplementedError(
            'You do not need a builder for SparklySession. '
            'Just use a regular python constructor. '
            'Please, follow the documentation for more details.'
        )

    def has_package(self, package_prefix):
        """Check if the package is available in the session.

        Args:
            package_prefix (str): E.g. "org.elasticsearch:elasticsearch-spark".

        Returns:
            bool
        """
        return any(package for package in self.packages if package.startswith(package_prefix))

    def has_jar(self, jar_name):
        """Check if the jar is available in the session.

        Args:
            jar_name (str): E.g. "mysql-connector-java".

        Returns:
            bool
        """
        return any(jar for jar in self.jars if jar_name in jar)

    def _setup_repositories(self):
        if self.repositories:
            return '--repositories {}'.format(','.join(self.repositories))
        else:
            return ''

    def _setup_packages(self):
        if self.packages:
            return '--packages {}'.format(','.join(self.packages))
        else:
            return ''

    def _setup_jars(self):
        if self.jars:
            return '--jars {}'.format(','.join(self.jars))
        else:
            return ''

    def _setup_options(self, additional_options):
        options = list(self.options.items())
        if additional_options:
            options += list(additional_options.items())

        return sorted(options)

    def _setup_udfs(self):
        for name, defn in self.udfs.items():
            if isinstance(defn, str):
                self.sql('create temporary function {} as "{}"'.format(name, defn))
            elif isinstance(defn, tuple):
                self.catalog.registerFunction(name, *defn)
            else:
                raise NotImplementedError('Incorrect UDF definition: {}: {}'.format(name, defn))
