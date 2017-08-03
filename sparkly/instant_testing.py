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

import json
import logging
import os
import signal
import tempfile

from py4j.java_gateway import java_import
from pyspark import SparkContext
from pyspark.java_gateway import launch_gateway


logger = logging.getLogger(__name__)


class InstantTesting(object):
    """The set of tools to run tests using Spark Context running in the background.

    Implementation:
        We create a lock file that will contain Python gateway port (exposed by JVM).

        On the first run:
            - initialise Spark Context as usual;
            - write Python gateway port to the lock file;
            - fork current process.

        On the second run:
            - connect to the background JVM process using Python gateway port from the lock file;
            - recover Spark Context from JVM.
    """
    LOCK_FILE_PATH = os.path.join(tempfile.gettempdir(), 'sparkly_instant_testing_lock')

    @classmethod
    def activate(cls):
        """Activate instant testing mode."""
        if os.path.exists(cls.LOCK_FILE_PATH):
            logger.error('Instant testing mode is already activate, deactivate it first.')
        else:
            with open(cls.LOCK_FILE_PATH, 'w'):
                logger.info('Instant testing mode has been activated.')

    @classmethod
    def deactivate(cls):
        """Deactivate instance testing mode."""
        if not os.path.exists(cls.LOCK_FILE_PATH):
            logger.error('Instant testing mode is not activated, activate it first.')
        else:
            try:
                with open(cls.LOCK_FILE_PATH) as lock:
                    state = lock.read()
                    if state:
                        session_pid = json.loads(state)['session_pid']
                        try:
                            os.kill(session_pid, signal.SIGTERM)
                        except OSError:
                            logger.exception(
                                'Can not kill background SparkContext (pid %d)', session_pid,
                            )
                        else:
                            logger.info(
                                'Killed background SparkContext (pid %d)', session_pid,
                            )
            finally:
                try:
                    os.remove(cls.LOCK_FILE_PATH)
                except OSError:
                    logger.exception('Can not remove lock file: %s', cls.LOCK_FILE_PATH)

            logger.info('Instant testing mode has been deactivated.')

    @classmethod
    def is_activated(cls):
        """Check if instant testing has been activated before.

        Returns:
            bool
        """
        return os.path.exists(cls.LOCK_FILE_PATH)

    @classmethod
    def set_context(cls, spark_context):
        """Set the given spark context for instant testing.

        Args:
            spark_context (pyspark.SparkContext)
        """
        assert cls.is_activated()

        gateway_port = spark_context._gateway.java_gateway_server.getListeningPort()

        # pid of the python process that holds JVM with running Spark Context.
        session_pid = os.getpid()

        with open(cls.LOCK_FILE_PATH, 'w') as lock:
            json.dump({'gateway_port': gateway_port, 'session_pid': session_pid}, lock)
            logger.info(
                'Successfully set spark context for the instant testing [pid=%s, gateway=%s]',
                session_pid, gateway_port
            )

    @classmethod
    def get_context(cls):
        """Get the current global spark context.

        Returns:
            pyspark.SparkContext or None (if wasn't set before).
        """
        assert cls.is_activated()

        state = None

        with open(cls.LOCK_FILE_PATH) as lock:
            serialised_state = lock.read()
            if serialised_state:
                try:
                    state = json.loads(serialised_state)
                except ValueError:
                    logger.error(
                        'Unable to deserialize lock file. Try to reactivate instant testing. '
                        'The broken content is: %s',
                        serialised_state,
                    )

        if state:
            logger.info(
                'Recovering context for the instant testing [pid=%s, gateway=%s]',
                state['session_pid'], state['gateway_port'],
            )

            os.environ['PYSPARK_GATEWAY_PORT'] = str(state['gateway_port'])
            gateway = launch_gateway()
            java_import(gateway.jvm, 'org.apache.spark.SparkContext')
            jvm_spark_context = gateway.jvm.SparkContext.getOrCreate()
            jvm_java_spark_context = gateway.jvm.JavaSparkContext(jvm_spark_context)

            SparkContext._gateway = gateway
            SparkContext._jvm = gateway.jvm

            return SparkContext(
                appName=jvm_spark_context.appName(),
                master=jvm_spark_context.master(),
                gateway=gateway,
                jsc=jvm_java_spark_context,
            )
