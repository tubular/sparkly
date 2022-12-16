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

import atexit
from copy import deepcopy
import os
import signal
import sys
import time
import uuid

from pyspark import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

from sparkly.catalog import SparklyCatalog
from sparkly.instant_testing import InstantTesting
from sparkly.reader import SparklyReader
from sparkly.writer import attach_writer_to_dataframe


class SparklySession(SparkSession):
    """Wrapper around SparkSession to simplify definition of options, packages, JARs and UDFs.

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

        # Alternatively
        spark = MySession.get_or_create()
        spark.read_ext.cassandra(...)

    Attributes:
        options (dict[str,str]): Configuration options that are passed to spark-submit.
            See `the list of possible options
            <https://spark.apache.org/docs/2.1.0/configuration.html#available-properties>`_.
            Note that any options set already through PYSPARK_SUBMIT_ARGS will override
            these.
        repositories (list[str]): List of additional maven repositories for package lookup.
        packages (list[str]): Spark packages that should be installed.
            See https://spark-packages.org/
        jars (list[str]): Full paths to jar files that we want to include to the session.
            E.g. a JDBC connector or a library with UDF functions.
        udfs (dict[str,str|typing.Callable]): Register UDF functions within the session.
            Key - a name of the function,
            Value - either a class name imported from a JAR file
                or a tuple with python function and its return type.
        name (str): a name that is used in default app_id_template (see below)
        app_id_template (str|None): if set and nonempty, generate the `spark.app.id` with
            this template. Interpolation is available with some pre-defined variables:
                * initial_time: the time that the first session started
                * initial_uid: a unique id associated with the first session
                * session_time: the time the session started
                * session_uid: a unique id associated with the session
            A default value is provided using the name, initial-uid and session-time.
            This helps a specific use case when running in Kubernetes: when a session
            is restarted, the same app-id is used, breaking storage of spark-history data
            (only the first session will have its history stored, unless overwrite mode
            is used, in which case only the last session will have its history stored).
            By defaulting to using the initial-uid and session-time information, we get
            sane "grouping" of all sessions originating from the same initial session, but also
            achieve separate individual app ids so that history for each can be maintained.
            To disable this functionality entirely, simply set to None or emptystring.
            Finally, if a user manually specifies `spark.app.id`, then that value will
            always trump any template provided here.
    """
    name = 'sparkly'
    options = {}
    packages = []
    jars = []
    udfs = {}
    repositories = []
    app_id_template = '{name}-{initial_uid}-{session_time}'

    _instantiated_session = None
    _original_environment = None

    _initial_time = None
    _initial_uid = None

    def __init__(self, additional_options=None):
        SparklySession._original_environment = deepcopy(os.environ)
        os.environ['PYSPARK_PYTHON'] = sys.executable

        self._initial_time = self._initial_time or int(time.time())
        self._initial_uid = self._initial_uid or uuid.uuid4().hex
        self._session_time = int(time.time())
        self._session_uid = uuid.uuid4().hex

        options = {
            'spark.sql.catalogImplementation': 'hive',
        }
        app_id_template = self.app_id_template
        if app_id_template:
            options.update({
                'spark.app.id': app_id_template.format(
                    name=self.name,
                    initial_time=self._initial_time,
                    initial_uid=self._initial_uid,
                    session_time=self._session_time,
                    session_uid=self._session_uid,
                ),
            })
        options.update(self.options or {})
        options.update(additional_options or {})
        options = {str(key): str(value) for key, value in options.items()}

        submit_args = [
            # options that were already defined through PYSPARK_SUBMIT_ARGS
            # take precedence over SparklySession's
            os.environ.get('PYSPARK_SUBMIT_ARGS', '').replace('pyspark-shell', ''),
            self._setup_repositories(),
            self._setup_packages(),
            self._setup_jars(),
            self._setup_options(options),
            'pyspark-shell',
        ]
        os.environ['PYSPARK_SUBMIT_ARGS'] = ' '.join(filter(None, submit_args))

        def get_context():
            conf = SparkConf()
            conf.setAll(options.items())
            return SparkContext(conf=conf)

        # If we are in instant testing mode
        if InstantTesting.is_activated():
            context = InstantTesting.get_context()

            # It's the first run, so we have to create context and demonise the process.
            if context is None:
                context = get_context()
                if os.fork() == 0:  # Detached process.
                    signal.pause()
                else:
                    InstantTesting.set_context(context)
        else:
            context = get_context()

        super(SparklySession, self).__init__(context)

        # similar to session builder:
        for key, value in options.items():
            self._jsparkSession.sessionState().conf().setConfString(key, value)

        self._setup_udfs()

        self.read_ext = SparklyReader(self)
        self.catalog_ext = SparklyCatalog(self)

        attach_writer_to_dataframe()
        SparklySession._instantiated_session = self

    @classmethod
    def get_or_create(cls):
        """Access instantiated sparkly session.

        If sparkly session has already been instantiated, return that
        instance; if not, then instantiate one and return it. Useful
        for lazy access to the session. Not thread-safe.

        Returns:
            SparklySession (or subclass).
        """
        if SparklySession._instantiated_session is None:
            cls()
        return SparklySession._instantiated_session

    @classmethod
    def stop(cls):
        """Stop instantiated sparkly session."""
        if SparklySession._instantiated_session is not None:
            SparkSession.stop(SparklySession._instantiated_session)
            SparklySession._instantiated_session = None
            os.environ = SparklySession._original_environment
            SparklySession._original_environment = None

    @property
    def builder(self):
        raise NotImplementedError(
            'You do not need a builder for SparklySession. '
            'Just use a regular python constructor. '
            'Please, follow the documentation for more details.'
        )

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

    def _setup_options(self, options):
        # Here we massage conf properties with the intent to pass them to
        # spark-submit; this is convenient as it is unified with the approach
        # we take for repos, packages and jars, and it also handles precedence
        # of conf properties already defined by the user in a very
        # straightforward way (since we always append to PYSPARK_SUBMIT_ARGS)
        return ' '.join('--conf "{}={}"'.format(*o) for o in sorted(options.items()))

    def _setup_udfs(self):
        for name, defn in self.udfs.items():
            if isinstance(defn, str):
                self.sql('create temporary function {} as "{}"'.format(name, defn))
            elif isinstance(defn, tuple):
                self.udf.register(name, *defn)
            else:
                raise NotImplementedError('Incorrect UDF definition: {}: {}'.format(name, defn))


# https://issues.apache.org/jira/browse/SPARK-27927
# Spark on Kubernetes has an issue where the python process finishes,
# but the controlling java process just hangs, so nothing terminates.
# There is a simple workaround to stop the session prior to python termination.
# We do that here with an atexit registration.
atexit.register(SparklySession.stop)
