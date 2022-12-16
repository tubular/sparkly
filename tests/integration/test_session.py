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

from sparkly.testing import SparklyGlobalSessionTest
from tests.integration.base import SparklyTestSession


class TestSparklySession(SparklyGlobalSessionTest):
    session = SparklyTestSession

    def test_options(self):
        self.assertEqual('hive', self.spark.conf.get('spark.sql.catalogImplementation'))
        self.assertEqual('117', self.spark.conf.get('my.custom.option.1'))
        self.assertEqual('223', self.spark.conf.get('my.custom.option.2'))
        self.assertEqual('333', self.spark.conf.get('my.custom.option.3'))

    def test_python_udf(self):
        rows = self.spark.sql('select length_of_text("hello world")')
        self.assertEqual(rows.collect()[0][0], '11')

    def test_jar_udf(self):
        self.spark.createDataFrame(
            [
                {'key_field': 'A', 'value_field': 1},
                {'key_field': 'B', 'value_field': 2},
                {'key_field': 'C', 'value_field': 3},
                {'key_field': 'D', 'value_field': 4},
            ],
        ).registerTempTable('test_jar_udf')

        rows = self.spark.sql('select collect(key_field, value_field) from test_jar_udf')
        self.assertEqual(rows.collect()[0][0], {'A': 1, 'B': 2, 'C': 3, 'D': 4})

    def test_builder(self):
        with self.assertRaises(NotImplementedError):
            assert self.spark.builder
