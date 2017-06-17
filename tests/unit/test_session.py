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

import sys
import unittest
try:
    from unittest import mock
except ImportError:
    import mock

from pyspark import SparkConf, SparkContext

from sparkly import SparklySession


os_mock = mock.Mock(**{
    'environ': {},
    'path.exists.return_value': False,
})


class TestSparklySession(unittest.TestCase):
    def setUp(self):
        super(TestSparklySession, self).setUp()
        self.spark_conf_mock = mock.Mock(spec=SparkConf)
        self.spark_context_mock = mock.Mock(spec=SparkContext)

        self.patches = [
            mock.patch('sparkly.session.SparkConf', self.spark_conf_mock),
            mock.patch('sparkly.session.SparkContext', self.spark_context_mock),
        ]
        [p.start() for p in self.patches]

    def tearDown(self):
        [p.stop() for p in self.patches]
        super(TestSparklySession, self).tearDown()

    def test_has_package(self):
        hc = SparklySession()
        self.assertFalse(hc.has_package('datastax:spark-cassandra-connector'))

        hc.packages = ['datastax:spark-cassandra-connector:1.6.1-s_2.10']
        self.assertTrue(hc.has_package('datastax:spark-cassandra-connector'))

    def test_has_jar(self):
        hc = SparklySession()
        self.assertFalse(hc.has_jar('mysql-connector-java'))

        hc.jars = ['mysql-connector-java-5.1.39-bin.jar']
        self.assertTrue(hc.has_jar('mysql-connector-java'))

    @mock.patch('sparkly.session.os', os_mock)
    def test_session_with_packages(self):
        class _Session(SparklySession):
            packages = ['package1', 'package2']

        _Session()

        self.assertEqual(os_mock.environ, {
            'PYSPARK_PYTHON': sys.executable,
            'PYSPARK_SUBMIT_ARGS': '--packages package1,package2  pyspark-shell',
        })

    @mock.patch('sparkly.session.os', os_mock)
    def test_session_with_jars(self):
        class _Session(SparklySession):
            jars = ['file_a.jar', 'file_b.jar']

        _Session()

        self.assertEqual(os_mock.environ, {
            'PYSPARK_PYTHON': sys.executable,
            'PYSPARK_SUBMIT_ARGS': ' --jars file_a.jar,file_b.jar pyspark-shell',
        })

    def test_session_with_options(self):
        class _Session(SparklySession):
            options = {
                'spark.option.a': 'value_a',
                'spark.option.b': 'value_b',
            }

        _Session(additional_options={'spark.option.c': 'value_c'})

        self.spark_conf_mock.return_value.setAll.assert_called_once_with([
            ('spark.option.a', 'value_a'),
            ('spark.option.b', 'value_b'),
            ('spark.option.c', 'value_c'),
        ])

    @mock.patch('sparkly.session.os', os_mock)
    def test_session_without_packages_jars_and_options(self):
        SparklySession()

        self.assertEqual(os_mock.environ, {
            'PYSPARK_PYTHON': sys.executable,
            'PYSPARK_SUBMIT_ARGS': '  pyspark-shell',
        })

    def test_broken_udf(self):
        class _Session(SparklySession):
            udfs = {
                'my_udf': {'unsupported format of udf'},
            }

        self.assertRaises(NotImplementedError, _Session)
