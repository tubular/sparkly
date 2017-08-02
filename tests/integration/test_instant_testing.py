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
import os

try:
    from unittest import mock
except ImportError:
    import mock

from pyspark import SparkContext

from sparkly.instant_testing import InstantTesting
from sparkly.testing import SparklyGlobalSessionTest
from tests.integration.base import SparklyTestSession


_MOCK_LOCK_FILE_PATH = InstantTesting.LOCK_FILE_PATH + '__test'


@mock.patch.object(InstantTesting, 'LOCK_FILE_PATH', _MOCK_LOCK_FILE_PATH)
class TestInstantTesting(SparklyGlobalSessionTest):
    session = SparklyTestSession

    def setUp(self):
        try:
            os.remove(_MOCK_LOCK_FILE_PATH)
        except:
            pass

    def test_set_context(self):
        InstantTesting.activate()
        InstantTesting.set_context(self.spark.sparkContext)

        with open(_MOCK_LOCK_FILE_PATH) as f:
            state = json.load(f)
            self.assertEqual(state, {
                'gateway_port':
                    self.spark.sparkContext._gateway.java_gateway_server.getListeningPort(),
                'session_pid': os.getpid(),
            })

    def test_get_context(self):
        initial_context = self.spark.sparkContext

        InstantTesting.activate()
        InstantTesting.set_context(initial_context)

        with mock.patch.object(SparkContext, '_active_spark_context', None):
            recovered_context = InstantTesting.get_context()

            self.assertIsInstance(recovered_context, SparkContext)
            self.assertEqual(initial_context.appName, recovered_context.appName)
            self.assertEqual(initial_context.master, recovered_context.master)
