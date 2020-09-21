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

from pyspark import SparkContext

from sparkly import SparklySession


class TestSparklySession(unittest.TestCase):

    maxDiff = None

    def setUp(self):
        super(TestSparklySession, self).setUp()
        self.spark_context_mock = mock.Mock(spec=SparkContext)

        self.patches = [
            mock.patch('sparkly.session.SparkContext', self.spark_context_mock),
        ]
        [p.start() for p in self.patches]

    def tearDown(self):
        [p.stop() for p in self.patches]
        super(TestSparklySession, self).tearDown()

    @mock.patch('sparkly.session.os')
    def test_session_with_packages(self, os_mock):
        os_mock.environ = {}

        class _Session(SparklySession):
            packages = ['package1', 'package2']

        _Session()

        self.assertEqual(os_mock.environ, {
            'PYSPARK_PYTHON': sys.executable,
            'PYSPARK_SUBMIT_ARGS': (
                '--packages package1,package2 '
                '--conf "spark.sql.catalogImplementation=hive" '
                'pyspark-shell'
            ),
        })

    @mock.patch('sparkly.session.os')
    def test_session_with_repositories(self, os_mock):
        os_mock.environ = {}

        class _Session(SparklySession):
            packages = ['package1', 'package2']
            repositories = [
                'http://my.maven.repo',
                'http://another.maven.repo',
            ]

        _Session()

        self.assertEqual(os_mock.environ, {
            'PYSPARK_PYTHON': sys.executable,
            'PYSPARK_SUBMIT_ARGS': (
                '--repositories http://my.maven.repo,http://another.maven.repo '
                '--packages package1,package2 '
                '--conf "spark.sql.catalogImplementation=hive" '
                'pyspark-shell'
            ),
        })

    @mock.patch('sparkly.session.os')
    def test_session_with_jars(self, os_mock):
        os_mock.environ = {}

        class _Session(SparklySession):
            jars = ['file_a.jar', 'file_b.jar']

        _Session()

        self.assertEqual(os_mock.environ, {
            'PYSPARK_PYTHON': sys.executable,
            'PYSPARK_SUBMIT_ARGS': (
                '--jars file_a.jar,file_b.jar '
                '--conf "spark.sql.catalogImplementation=hive" '
                'pyspark-shell'
            ),
        })

    @mock.patch('sparkly.session.os')
    def test_session_with_options(self, os_mock):
        os_mock.environ = {}

        # test options attached to class definition
        class _Session(SparklySession):
            options = {
                'spark.option.a': 'value_a',
                'spark.option.b': 'value_b',
            }

        _Session()

        self.assertEqual(os_mock.environ, {
            'PYSPARK_PYTHON': sys.executable,
            'PYSPARK_SUBMIT_ARGS': (
                '--conf "spark.option.a=value_a" '
                '--conf "spark.option.b=value_b" '
                '--conf "spark.sql.catalogImplementation=hive" '
                'pyspark-shell'
            ),
        })

        # test additional_options override/extend options attached to class definition
        os_mock.environ = {}

        _Session(additional_options={
            'spark.option.b': 'value_0',
            'spark.option.c': 'value_c',
        })

        self.assertEqual(os_mock.environ, {
            'PYSPARK_PYTHON': sys.executable,
            'PYSPARK_SUBMIT_ARGS': (
                '--conf "spark.option.a=value_a" '
                '--conf "spark.option.b=value_0" '
                '--conf "spark.option.c=value_c" '
                '--conf "spark.sql.catalogImplementation=hive" '
                'pyspark-shell'
            ),
        })

        # test catalog implementation is respected
        os_mock.environ = {}

        _Session.options = {
            'spark.sql.catalogImplementation': 'my_fancy_catalog',
        }

        _Session()

        self.assertEqual(os_mock.environ, {
            'PYSPARK_PYTHON': sys.executable,
            'PYSPARK_SUBMIT_ARGS': (
                '--conf "spark.sql.catalogImplementation=my_fancy_catalog" '
                'pyspark-shell'
            ),
        })

    @mock.patch('sparkly.session.os')
    def test_session_without_packages_jars_and_options(self, os_mock):
        os_mock.environ = {}

        SparklySession()

        self.assertEqual(os_mock.environ, {
            'PYSPARK_PYTHON': sys.executable,
            'PYSPARK_SUBMIT_ARGS': '--conf "spark.sql.catalogImplementation=hive" pyspark-shell',
        })

    @mock.patch('sparkly.session.os')
    def test_session_appends_to_pyspark_submit_args(self, os_mock):
        os_mock.environ = {
            'PYSPARK_SUBMIT_ARGS': '--conf "my.conf.here=5g" --and-other-properties',
        }

        SparklySession()

        self.assertEqual(os_mock.environ, {
            'PYSPARK_PYTHON': sys.executable,
            'PYSPARK_SUBMIT_ARGS': (
                '--conf "my.conf.here=5g" --and-other-properties '
                '--conf "spark.sql.catalogImplementation=hive" '
                'pyspark-shell'
            ),
        })

        # test more complicated session
        os_mock.environ = {
            'PYSPARK_SUBMIT_ARGS': '--conf "my.conf.here=5g" --and-other-properties',
        }

        class _Session(SparklySession):
            options = {'my.conf.here': '10g'}

        _Session()

        self.assertEqual(os_mock.environ, {
            'PYSPARK_PYTHON': sys.executable,
            'PYSPARK_SUBMIT_ARGS': (
                '--conf "my.conf.here=5g" --and-other-properties '
                # Note that spark honors the first conf it sees when multiple
                # are defined
                '--conf "my.conf.here=10g" '
                '--conf "spark.sql.catalogImplementation=hive" '
                'pyspark-shell'
            ),
        })

    def test_broken_udf(self):
        class _Session(SparklySession):
            udfs = {
                'my_udf': {'unsupported format of udf'},
            }

        self.assertRaises(NotImplementedError, _Session)

    @mock.patch('sparkly.session.SparkSession')
    def test_get_or_create_and_stop(self, spark_session_mock):
        # Not a great practice to test two functions in one unit test,
        # but get_or_create and stop are kind of intertwined with each other

        class _Session(SparklySession):
            pass

        # check stopping a running session
        original_session = _Session()
        _Session.stop()
        spark_session_mock.stop.assert_called_once_with(original_session)

        # check that stopping when there's no session has no impact
        _Session.stop()
        spark_session_mock.stop.assert_called_once_with(original_session)

        # check creating a new session thru get_or_create
        retrieved_session = _Session.get_or_create()
        self.assertNotEqual(id(retrieved_session), id(original_session))

        # check retrieving a session thru get_or_create
        original_session = _Session()
        retrieved_session = _Session.get_or_create()
        self.assertEqual(id(retrieved_session), id(original_session))

        # check retrieving a session thru SparklySession.get_or_create
        original_session = _Session()
        retrieved_session = SparklySession.get_or_create()
        self.assertEqual(id(retrieved_session), id(original_session))

    @mock.patch('sparkly.session.os')
    @mock.patch('sparkly.session.SparkSession')
    def test_stop_restores_the_environment(self, spark_session_mock, os_mock):
        os_mock.environ = {
            'PYSPARK_SUBMIT_ARGS': '--conf "my.conf.here=5g" --and-other-properties',
        }

        SparklySession()
        SparklySession.stop()

        self.assertEqual(os_mock.environ, {
            'PYSPARK_SUBMIT_ARGS': '--conf "my.conf.here=5g" --and-other-properties',
        })
