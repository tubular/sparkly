import sys
import unittest
try:
    from unittest import mock
except ImportError:
    import mock

from pyspark import SparkConf, SparkContext

from sparkle import SparkleContext


class TestSparkleContext(unittest.TestCase):
    def setUp(self):
        super(TestSparkleContext, self).setUp()
        self.spark_conf_mock = mock.Mock(spec=SparkConf)
        self.spark_context_mock = mock.Mock(spec=SparkContext)

        self.patches = [
            mock.patch('sparkle.context.SparkConf', self.spark_conf_mock),
            mock.patch('sparkle.context.SparkContext', self.spark_context_mock),
        ]
        [p.start() for p in self.patches]

    def tearDown(self):
        [p.stop() for p in self.patches]
        super(TestSparkleContext, self).tearDown()

    def test_has_package(self):
        hc = SparkleContext()
        self.assertFalse(hc.has_package('datastax:spark-cassandra-connector'))

        hc.packages = ['datastax:spark-cassandra-connector:1.6.1-s_2.10']
        self.assertTrue(hc.has_package('datastax:spark-cassandra-connector'))

    def test_has_jar(self):
        hc = SparkleContext()
        self.assertFalse(hc.has_jar('mysql-connector-java'))

        hc.jars = ['mysql-connector-java-5.1.39-bin.jar']
        self.assertTrue(hc.has_jar('mysql-connector-java'))

    @mock.patch('sparkle.context.os')
    def test_context_with_packages(self, os_mock):
        os_mock.environ = {}

        class _Context(SparkleContext):
            packages = ['package1', 'package2']

        _Context()

        self.assertEqual(os_mock.environ, {
            'PYSPARK_PYTHON': sys.executable,
            'PYSPARK_SUBMIT_ARGS': '--packages package1,package2  pyspark-shell',
        })

    @mock.patch('sparkle.context.os')
    def test_context_with_jars(self, os_mock):
        os_mock.environ = {}

        class _Context(SparkleContext):
            jars = ['file_a.jar', 'file_b.jar']

        _Context()

        self.assertEqual(os_mock.environ, {
            'PYSPARK_PYTHON': sys.executable,
            'PYSPARK_SUBMIT_ARGS': ' --jars file_a.jar,file_b.jar pyspark-shell',
        })

    def test_context_with_options(self):
        class _Context(SparkleContext):
            options = {
                'spark.option.a': 'value_a',
                'spark.option.b': 'value_b',
            }

        _Context(additional_options={'spark.option.c': 'value_c'})

        self.spark_conf_mock.return_value.setAll.assert_called_once_with([
            ('spark.option.a', 'value_a'),
            ('spark.option.b', 'value_b'),
            ('spark.option.c', 'value_c'),
        ])

    @mock.patch('sparkle.context.os')
    def test_context_without_packages_jars_and_options(self, os_mock):
        os_mock.environ = {}

        SparkleContext()

        self.assertEqual(os_mock.environ, {
            'PYSPARK_PYTHON': sys.executable,
            'PYSPARK_SUBMIT_ARGS': '  pyspark-shell',
        })

    def test_broken_udf(self):
        class _Context(SparkleContext):
            udfs = {
                'my_udf': {'unsupported format of udf'},
            }

        self.assertRaises(NotImplementedError, _Context)
