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

from functools import partial
import gzip
import uuid
from shutil import rmtree
from tempfile import mkdtemp
from time import sleep
import zlib

from pyspark.sql import functions as F
from pyspark.sql import types as T
from py4j.protocol import Py4JJavaError
import redis
import six
import ujson as json

from sparkly.utils import absolute_path
from sparkly.testing import (
    SparklyTest,
    SparklyGlobalSessionTest,
    CassandraFixture,
    ElasticFixture,
    MysqlFixture,
)
from tests.integration.base import (
    SparklyTestSession,
    SparklyTestSessionWithES6,
)

try:
    from kafka import KafkaConsumer
except ImportError:
    pass


TEST_DATA = [
    {'uid': 'v1', 'title': 'Video A', 'views': 1000},
    {'uid': 'v2', 'title': 'Video B', 'views': 2000},
    {'uid': 'v3', 'title': 'Video C', 'views': 3000},
]


class TestWriteByURL(SparklyGlobalSessionTest):
    session = SparklyTestSession

    def setUp(self):
        self.temp_dir = mkdtemp()

    def tearDown(self):
        rmtree(self.temp_dir)

    def test_write_csv(self):
        dst_path = '{}/test_csv'.format(self.temp_dir)
        df = self.spark.createDataFrame(TEST_DATA)

        df.write_ext.by_url('csv://{}?mode=overwrite&header=true'.format(dst_path))

        written_df = self.spark.read_ext.by_url('csv://{}?header=true&inferSchema=true'
                                                .format(dst_path))
        self.assertDataFrameEqual(written_df, TEST_DATA)

    def test_write_parquet(self):
        dst_path = '{}/test_parquet'.format(self.temp_dir)
        df = self.spark.createDataFrame(TEST_DATA)

        df.write_ext.by_url('parquet://{}?mode=overwrite'.format(dst_path))

        written_df = self.spark.read_ext.by_url('parquet://{}'.format(dst_path))
        self.assertDataFrameEqual(written_df, TEST_DATA)


class TestWriteCassandra(SparklyGlobalSessionTest):
    session = SparklyTestSession

    fixtures = [
        CassandraFixture(
            'cassandra.docker',
            absolute_path(__file__, 'resources', 'test_write', 'cassandra_setup.cql'),
            absolute_path(__file__, 'resources', 'test_write', 'cassandra_teardown.cql'),
        )
    ]

    def test_write_cassandra(self):
        df = self.spark.createDataFrame(TEST_DATA)

        df.write_ext.cassandra(
            host='cassandra.docker',
            port=9042,
            keyspace='sparkly_test',
            table='test_writer',
            consistency='ONE',
            mode='overwrite',
            # overwrite would first perform truncation.
            # Either change mode to 'append' to change data already in the table,
            # or set confirm.truncate to true
            options={'confirm.truncate': True},
        )

        written_df = self.spark.read_ext.by_url(
            'cassandra://cassandra.docker/'
            'sparkly_test/test_writer'
            '?consistency=ONE'
        )
        self.assertDataFrameEqual(written_df, TEST_DATA)


class TestWriteElastic6(SparklyTest):
    session = SparklyTestSessionWithES6

    fixtures = [
        ElasticFixture(
            'elastic6.docker',
            'sparkly_test',
            'test',
            None,
            absolute_path(__file__, 'resources', 'test_write', 'elastic_setup.json')
        )
    ]

    def test_write_elastic(self):
        df = self.spark.createDataFrame(TEST_DATA)

        df.write_ext.elastic(
            host='elastic6.docker',
            port=9200,
            es_index='sparkly_test',
            es_type='test_writer',
            mode='overwrite',
            options={
                'es.mapping.id': 'uid',
            }
        )

        df = self.spark.read_ext.by_url(
            'elastic://elastic6.docker/sparkly_test/test_writer?es.read.metadata=false'
        )
        self.assertDataFrameEqual(df, TEST_DATA)


class TestWriteElastic7(SparklyGlobalSessionTest):
    session = SparklyTestSession

    fixtures = [
        ElasticFixture(
            'elastic7.docker',
            'sparkly_test',
            None,
            None,
            absolute_path(__file__, 'resources', 'test_write', 'elastic7_setup.json'),
        ),
    ]

    def test_write_elastic(self):
        df = self.spark.createDataFrame(TEST_DATA)

        df.write_ext.elastic(
            host='elastic7.docker',
            port=9200,
            es_index='sparkly_test',
            es_type=None,
            mode='overwrite',
            options={
                'es.mapping.id': 'uid',
            },
        )

        df = self.spark.read_ext.by_url(
            'elastic://elastic7.docker/sparkly_test?es.read.metadata=false',
        )
        self.assertDataFrameEqual(df, TEST_DATA)


class TestWriteMysql(SparklyGlobalSessionTest):
    session = SparklyTestSession

    fixtures = [
        MysqlFixture(
            'mysql.docker',
            'root',
            None,
            absolute_path(__file__, 'resources', 'test_write', 'mysql_setup.sql'),
            absolute_path(__file__, 'resources', 'test_write', 'mysql_teardown.sql'),
        )
    ]

    def test_write_mysql(self):
        df = self.spark.createDataFrame(TEST_DATA)

        df.write_ext.mysql(
            host='mysql.docker',
            port=3306,
            database='sparkly_test',
            table='test_writer',
            mode='overwrite',
            options={'user': 'root', 'password': ''}
        )

        df = self.spark.read_ext.by_url(
            'mysql://mysql.docker/'
            'sparkly_test/test_writer'
            '?user=root&password='
        )
        self.assertDataFrameEqual(df, TEST_DATA)


class TestWriteKafka(SparklyGlobalSessionTest):
    session = SparklyTestSession

    def setUp(self):
        self.json_decoder = lambda item: json.loads(item.decode('utf-8'))
        self.json_encoder = lambda item: json.dumps(item).encode('utf-8')
        self.topic = 'test.topic.write.kafka.{}'.format(uuid.uuid4().hex[:10])
        self.fixture_path = absolute_path(__file__, 'resources', 'test_write', 'kafka_setup.json')
        self.expected_data = self.spark.read.json(self.fixture_path)

    def test_write_kafka_dataframe(self):
        self.expected_data.write_ext.kafka(
            'kafka.docker',
            self.topic,
            key_serializer=self.json_encoder,
            value_serializer=self.json_encoder,
        )

        consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers='kafka.docker:9092',
            key_deserializer=lambda item: json.loads(item.decode('utf-8')),
            value_deserializer=lambda item: json.loads(item.decode('utf-8')),
            auto_offset_reset='earliest',
        )

        actual_data = []
        for i in range(self.expected_data.count()):
            message = next(consumer)
            data = {'key': message.key, 'value': message.value}
            actual_data.append(data)

        self.assertDataFrameEqual(self.expected_data, actual_data)

    def test_write_kafka_dataframe_error(self):
        def _errored_serializer(data):
            raise ValueError

        try:
            self.expected_data.write_ext.kafka(
                'kafka.docker',
                self.topic,
                key_serializer=_errored_serializer,
                value_serializer=_errored_serializer,
            )
        except Py4JJavaError as ex:
            self.assertIn('WriteError(\'Error publishing to kafka', str(ex))
        else:
            raise AssertionError('WriteError exception not raised')


class TestWriteRedis(SparklyGlobalSessionTest):
    session = SparklyTestSession

    def tearDown(self):
        redis.StrictRedis('redis.docker', 6379).flushdb()
        super(TestWriteRedis, self).tearDown()

    @staticmethod
    def _gzip_decompress(data):
        try:
            return gzip.decompress(data)
        except AttributeError:  #py27
            return zlib.decompress(data, zlib.MAX_WBITS | 16)

    def test_simple_key_uncompressed(self):
        df = self.spark.createDataFrame(
            data=[
                ('k1', 'k14', [1, 14, 141]),
                ('k1', 'k12', [1, 12, 121]),
                ('k1', 'k11', [1, 11, 111]),
                ('k1', 'k13', [1, 13, 131]),
            ],
            schema=T.StructType([
                T.StructField('key_1', T.StringType()),
                T.StructField('key_2', T.StringType()),
                T.StructField('aux_data', T.ArrayType(T.IntegerType())),
            ])
        )

        df.write_ext.redis(
            key_by=['key_2'],
            max_pipeline_size=3,
            host='redis.docker',
        )

        redis_client = redis.StrictRedis('redis.docker')

        self.assertRowsEqual(
            redis_client.keys(),
            [b'k11', b'k12', b'k13', b'k14'],
            ignore_order=True,
        )

        written_data = [
            json.loads(redis_client.get(key)) for key in ['k11', 'k12', 'k13', 'k14']
        ]

        expected = [
            {'key_1': 'k1', 'key_2': 'k11', 'aux_data': [1, 11, 111]},
            {'key_1': 'k1', 'key_2': 'k12', 'aux_data': [1, 12, 121]},
            {'key_1': 'k1', 'key_2': 'k13', 'aux_data': [1, 13, 131]},
            {'key_1': 'k1', 'key_2': 'k14', 'aux_data': [1, 14, 141]},
        ]

        self.assertEqual(written_data, expected)

    def test_composite_key_compressed(self):
        df = self.spark.createDataFrame(
            data=[
                ('k1', 'k14', [1, 14, 141]),
                ('k1', 'k12', [1, 12, 121]),
                ('k1', 'k11', [1, 11, 111]),
                ('k1', 'k13', [1, 13, 131]),
            ],
            schema=T.StructType([
                T.StructField('key_1', T.StringType()),
                T.StructField('key_2', T.StringType()),
                T.StructField('aux_data', T.ArrayType(T.IntegerType())),
            ])
        )

        df.write_ext.redis(
            key_by=['key_1', 'key_2'],
            compression='gzip',
            max_pipeline_size=3,
            host='redis.docker',
        )

        redis_client = redis.StrictRedis('redis.docker')

        self.assertRowsEqual(
            redis_client.keys(),
            [b'k1.k11', b'k1.k12', b'k1.k13', b'k1.k14'],
            ignore_order=True,
        )

        written_data = [
            json.loads(self._gzip_decompress(redis_client.get(key)))
            for key in ['k1.k11', 'k1.k12', 'k1.k13', 'k1.k14']
        ]

        expected = [
            {'key_1': 'k1', 'key_2': 'k11', 'aux_data': [1, 11, 111]},
            {'key_1': 'k1', 'key_2': 'k12', 'aux_data': [1, 12, 121]},
            {'key_1': 'k1', 'key_2': 'k13', 'aux_data': [1, 13, 131]},
            {'key_1': 'k1', 'key_2': 'k14', 'aux_data': [1, 14, 141]},
        ]

        self.assertEqual(written_data, expected)

    def test_composite_key_with_prefix_compressed(self):
        df = self.spark.createDataFrame(
            data=[
                ('k1', 'k14', [1, 14, 141]),
                ('k1', 'k12', [1, 12, 121]),
                ('k1', 'k11', [1, 11, 111]),
                ('k1', 'k13', [1, 13, 131]),
            ],
            schema=T.StructType([
                T.StructField('key_1', T.StringType()),
                T.StructField('key_2', T.StringType()),
                T.StructField('aux_data', T.ArrayType(T.IntegerType())),
            ])
        )

        df.write_ext.redis(
            key_by=['key_1', 'key_2'],
            key_prefix='hello',
            compression='gzip',
            max_pipeline_size=3,
            host='redis.docker',
        )

        redis_client = redis.StrictRedis('redis.docker')

        self.assertRowsEqual(
            redis_client.keys(),
            [b'hello.k1.k11', b'hello.k1.k12', b'hello.k1.k13', b'hello.k1.k14'],
            ignore_order=True,
        )

        written_data = [
            json.loads(self._gzip_decompress(redis_client.get(key)))
            for key in ['hello.k1.k11', 'hello.k1.k12', 'hello.k1.k13', 'hello.k1.k14']
        ]

        expected = [
            {'key_1': 'k1', 'key_2': 'k11', 'aux_data': [1, 11, 111]},
            {'key_1': 'k1', 'key_2': 'k12', 'aux_data': [1, 12, 121]},
            {'key_1': 'k1', 'key_2': 'k13', 'aux_data': [1, 13, 131]},
            {'key_1': 'k1', 'key_2': 'k14', 'aux_data': [1, 14, 141]},
        ]

        self.assertEqual(written_data, expected)

    def test_group_by(self):
        df = self.spark.createDataFrame(
            data=[
                ('k4', 'k14', [1, 14, 141]),
                ('k1', 'k12', [1, 12, 121]),
                ('k1', 'k11', [1, 11, 111]),
                ('k1', 'k13', [1, 13, 131]),
            ],
            schema=T.StructType([
                T.StructField('key_1', T.StringType()),
                T.StructField('key_2', T.StringType()),
                T.StructField('aux_data', T.ArrayType(T.IntegerType())),
            ])
        )

        df.write_ext.redis(
            key_by=['key_1'],
            group_by_key=True,
            max_pipeline_size=2,
            host='redis.docker',
        )

        redis_client = redis.StrictRedis('redis.docker')

        self.assertRowsEqual(redis_client.keys(), [b'k1', b'k4'], ignore_order=True)

        written_data = [json.loads(redis_client.get(key)) for key in [b'k1', b'k4']]

        expected = [
            [
                {'key_1': 'k1', 'key_2': 'k11', 'aux_data': [1, 11, 111]},
                {'key_1': 'k1', 'key_2': 'k12', 'aux_data': [1, 12, 121]},
                {'key_1': 'k1', 'key_2': 'k13', 'aux_data': [1, 13, 131]},
            ],
            [{'key_1': 'k4', 'key_2': 'k14', 'aux_data': [1, 14, 141]}],
        ]

        self.assertRowsEqual(written_data, expected, ignore_order=True)

    def test_exclude_key_columns(self):
        df = self.spark.createDataFrame(
            data=[
                ('k1', 'k14', [1, 14, 141]),
                ('k1', 'k12', [1, 12, 121]),
                ('k1', 'k11', [1, 11, 111]),
                ('k1', 'k13', [1, 13, 131]),
            ],
            schema=T.StructType([
                T.StructField('key_1', T.StringType()),
                T.StructField('key_2', T.StringType()),
                T.StructField('aux_data', T.ArrayType(T.IntegerType())),
            ])
        )

        redis_client = redis.StrictRedis('redis.docker')

        # simple key
        df.write_ext.redis(
            key_by=['key_2'],
            key_prefix='hello',
            exclude_key_columns=True,
            host='redis.docker',
        )

        self.assertRowsEqual(
            redis_client.keys(),
            [b'hello.k11', b'hello.k12', b'hello.k13', b'hello.k14'],
            ignore_order=True,
        )

        written_data = [
            json.loads(redis_client.get(key))
            for key in ['hello.k11', 'hello.k12', 'hello.k13', 'hello.k14']
        ]

        expected = [
            {'key_1': 'k1', 'aux_data': [1, 11, 111]},
            {'key_1': 'k1', 'aux_data': [1, 12, 121]},
            {'key_1': 'k1', 'aux_data': [1, 13, 131]},
            {'key_1': 'k1', 'aux_data': [1, 14, 141]},
        ]

        self.assertEqual(written_data, expected)

        redis_client.flushdb()

        # composite key
        df.write_ext.redis(
            key_by=['key_1', 'key_2'],
            key_prefix='hello',
            exclude_key_columns=True,
            host='redis.docker',
        )

        self.assertRowsEqual(
            redis_client.keys(),
            [b'hello.k1.k11', b'hello.k1.k12', b'hello.k1.k13', b'hello.k1.k14'],
            ignore_order=True,
        )

        written_data = [
            json.loads(redis_client.get(key))
            for key in ['hello.k1.k11', 'hello.k1.k12', 'hello.k1.k13', 'hello.k1.k14']
        ]

        expected = [
            {'aux_data': [1, 11, 111]},
            {'aux_data': [1, 12, 121]},
            {'aux_data': [1, 13, 131]},
            {'aux_data': [1, 14, 141]},
        ]

        self.assertEqual(written_data, expected)

    def test_exclude_null_fields(self):
        df = self.spark.createDataFrame(
            data=[
                ('k1', 'k14', [1, 14, 141]),
                (None, 'k12', [1, 12, 121]),
                ('k1', 'k11', None),
                ('k1', 'k13', [1, 13, 131]),
            ],
            schema=T.StructType([
                T.StructField('key_1', T.StringType()),
                T.StructField('key_2', T.StringType()),
                T.StructField('aux_data', T.ArrayType(T.IntegerType())),
            ])
        )

        redis_client = redis.StrictRedis('redis.docker')

        df.write_ext.redis(
            key_by=['key_2'],
            key_prefix='hello',
            exclude_null_fields=True,
            host='redis.docker',
        )

        self.assertRowsEqual(
            redis_client.keys(),
            [b'hello.k11', b'hello.k12', b'hello.k13', b'hello.k14'],
            ignore_order=True,
        )

        written_data = [
            json.loads(redis_client.get(key))
            for key in ['hello.k11', 'hello.k12', 'hello.k13', 'hello.k14']
        ]

        expected = [
            {'key_1': 'k1', 'key_2': 'k11'},
            {'key_2': 'k12', 'aux_data': [1, 12, 121]},
            {'key_1': 'k1', 'key_2': 'k13', 'aux_data': [1, 13, 131]},
            {'key_1': 'k1', 'key_2': 'k14', 'aux_data': [1, 14, 141]},
        ]

        self.assertEqual(written_data, expected)

    def test_exclude_key_columns(self):
        df = self.spark.createDataFrame(
            data=[
                ('k1', 'k14', [1, 14, 141]),
                ('k1', 'k12', [1, 12, 121]),
                ('k1', 'k11', [1, 11, 111]),
                ('k1', 'k13', [1, 13, 131]),
            ],
            schema=T.StructType([
                T.StructField('key_1', T.StringType()),
                T.StructField('key_2', T.StringType()),
                T.StructField('aux_data', T.ArrayType(T.IntegerType())),
            ])
        )

        redis_client = redis.StrictRedis('redis.docker')

        # simple key
        df.write_ext.redis(
            key_by=['key_2'],
            key_prefix='hello',
            exclude_key_columns=True,
            host='redis.docker',
        )

        self.assertRowsEqual(
            redis_client.keys(),
            [b'hello.k11', b'hello.k12', b'hello.k13', b'hello.k14'],
            ignore_order=True,
        )

        written_data = [
            json.loads(redis_client.get(key))
            for key in ['hello.k11', 'hello.k12', 'hello.k13', 'hello.k14']
        ]

        expected = [
            {'key_1': 'k1', 'aux_data': [1, 11, 111]},
            {'key_1': 'k1', 'aux_data': [1, 12, 121]},
            {'key_1': 'k1', 'aux_data': [1, 13, 131]},
            {'key_1': 'k1', 'aux_data': [1, 14, 141]},
        ]

        self.assertEqual(written_data, expected)

        redis_client.flushdb()

        # composite key
        df.write_ext.redis(
            key_by=['key_1', 'key_2'],
            key_prefix='hello',
            exclude_key_columns=True,
            host='redis.docker',
        )

        self.assertRowsEqual(
            redis_client.keys(),
            [b'hello.k1.k11', b'hello.k1.k12', b'hello.k1.k13', b'hello.k1.k14'],
            ignore_order=True,
        )

        written_data = [
            json.loads(redis_client.get(key))
            for key in ['hello.k1.k11', 'hello.k1.k12', 'hello.k1.k13', 'hello.k1.k14']
        ]

        expected = [
            {'aux_data': [1, 11, 111]},
            {'aux_data': [1, 12, 121]},
            {'aux_data': [1, 13, 131]},
            {'aux_data': [1, 14, 141]},
        ]

        self.assertEqual(written_data, expected)

    def test_exclude_null_fields(self):
        df = self.spark.createDataFrame(
            data=[
                ('k1', 'k14', [1, 14, 141]),
                (None, 'k12', [1, 12, 121]),
                ('k1', 'k11', None),
                ('k1', 'k13', [1, 13, 131]),
            ],
            schema=T.StructType([
                T.StructField('key_1', T.StringType()),
                T.StructField('key_2', T.StringType()),
                T.StructField('aux_data', T.ArrayType(T.IntegerType())),
            ])
        )

        redis_client = redis.StrictRedis('redis.docker')

        df.write_ext.redis(
            key_by=['key_2'],
            key_prefix='hello',
            exclude_null_fields=True,
            host='redis.docker',
        )

        self.assertRowsEqual(
            redis_client.keys(),
            [b'hello.k11', b'hello.k12', b'hello.k13', b'hello.k14'],
            ignore_order=True,
        )

        written_data = [
            json.loads(redis_client.get(key))
            for key in ['hello.k11', 'hello.k12', 'hello.k13', 'hello.k14']
        ]

        expected = [
            {'key_1': 'k1', 'key_2': 'k11'},
            {'key_2': 'k12', 'aux_data': [1, 12, 121]},
            {'key_1': 'k1', 'key_2': 'k13', 'aux_data': [1, 13, 131]},
            {'key_1': 'k1', 'key_2': 'k14', 'aux_data': [1, 14, 141]},
        ]

        self.assertEqual(written_data, expected)

    def test_expiration(self):
        df = self.spark.createDataFrame(
            data=[
                ('k1', 'k14', [1, 14, 141]),
                ('k1', 'k12', [1, 12, 121]),
                ('k1', 'k11', [1, 11, 111]),
                ('k1', 'k13', [1, 13, 131]),
            ],
            schema=T.StructType([
                T.StructField('key_1', T.StringType()),
                T.StructField('key_2', T.StringType()),
                T.StructField('aux_data', T.ArrayType(T.IntegerType())),
            ])
        )

        df.write_ext.redis(
            key_by=['key_2'],
            key_prefix='hello',
            expire=2,
            host='redis.docker',
        )

        redis_client = redis.StrictRedis('redis.docker')

        self.assertRowsEqual(
            redis_client.keys(),
            [b'hello.k11', b'hello.k12', b'hello.k13', b'hello.k14'],
            ignore_order=True,
        )

        written_data = [
            json.loads(redis_client.get(key))
            for key in ['hello.k11', 'hello.k12', 'hello.k13', 'hello.k14']
        ]

        expected = [
            {'key_1': 'k1', 'key_2': 'k11', 'aux_data': [1, 11, 111]},
            {'key_1': 'k1', 'key_2': 'k12', 'aux_data': [1, 12, 121]},
            {'key_1': 'k1', 'key_2': 'k13', 'aux_data': [1, 13, 131]},
            {'key_1': 'k1', 'key_2': 'k14', 'aux_data': [1, 14, 141]},
        ]

        self.assertEqual(written_data, expected)

        sleep(3)

        self.assertEqual(redis_client.keys(), [])

    def test_mode(self):
        redis_client = redis.StrictRedis('redis.docker')
        redis_client.set('k11', '"hey!"')
        redis_client.set('k13', '"you!"')
        redis_client.set('k14', '"brick!"')

        df = self.spark.createDataFrame(
            data=[
                ('k1', 'k14', [1, 14, 141]),
                ('k1', 'k12', [1, 12, 121]),
                ('k1', 'k11', [1, 11, 111]),
                ('k1', 'k13', [1, 13, 131]),
            ],
            schema=T.StructType([
                T.StructField('key_1', T.StringType()),
                T.StructField('key_2', T.StringType()),
                T.StructField('aux_data', T.ArrayType(T.IntegerType())),
            ])
        )

        # test ignore
        df.write_ext.redis(
            key_by=['key_2'],
            mode='ignore',
            host='redis.docker',
        )

        self.assertRowsEqual(
            redis_client.keys(),
            [b'k11', b'k12', b'k13', b'k14'],
            ignore_order=True,
        )

        written_data = [
            json.loads(redis_client.get(key)) for key in ['k11', 'k12', 'k13', 'k14']
        ]

        expected = [
            'hey!',
            {'key_1': 'k1', 'key_2': 'k12', 'aux_data': [1, 12, 121]},
            'you!',
            'brick!',
        ]

        self.assertEqual(written_data, expected)

        # test append
        df.write_ext.redis(
            key_by=['key_2'],
            mode='append',
            host='redis.docker',
        )

        self.assertRowsEqual(
            redis_client.keys(),
            [b'k11', b'k12', b'k13', b'k14'],
            ignore_order=True,
        )

        written_data = [
            json.loads(redis_client.get(key)) for key in ['k11', 'k12', 'k13', 'k14']
        ]

        expected = [
            {'key_1': 'k1', 'key_2': 'k11', 'aux_data': [1, 11, 111]},
            {'key_1': 'k1', 'key_2': 'k12', 'aux_data': [1, 12, 121]},
            {'key_1': 'k1', 'key_2': 'k13', 'aux_data': [1, 13, 131]},
            {'key_1': 'k1', 'key_2': 'k14', 'aux_data': [1, 14, 141]},
        ]

        self.assertEqual(written_data, expected)

        # test overwrite
        df.where(F.col('key_2') == 'k11').write_ext.redis(
            key_by=['key_2'],
            mode='overwrite',
            host='redis.docker',
        )

        self.assertEqual(redis_client.keys(), [b'k11'])

        written_data = [json.loads(redis_client.get('k11'))]

        expected = [
            {'key_1': 'k1', 'key_2': 'k11', 'aux_data': [1, 11, 111]},
        ]

        self.assertEqual(written_data, expected)

    def test_db(self):
        df = self.spark.createDataFrame(
            data=[
                ('k1', 'k14', [1, 14, 141]),
            ],
            schema=T.StructType([
                T.StructField('key_1', T.StringType()),
                T.StructField('key_2', T.StringType()),
                T.StructField('aux_data', T.ArrayType(T.IntegerType())),
            ])
        )

        df.write_ext.redis(
            key_by=['key_2'],
            max_pipeline_size=3,
            host='redis.docker',
            db=1,
        )

        redis_client = redis.StrictRedis('redis.docker', db=1)

        self.assertEqual(redis_client.keys(), [b'k14'])

        written_data = json.loads(redis_client.get('k14'))
        expected = {'key_1': 'k1', 'key_2': 'k14', 'aux_data': [1, 14, 141]}
        self.assertEqual(written_data, expected)

    def test_redis_client_init(self):
        df = self.spark.createDataFrame(
            data=[
                ('k1', 'k14', [1, 14, 141]),
            ],
            schema=T.StructType([
                T.StructField('key_1', T.StringType()),
                T.StructField('key_2', T.StringType()),
                T.StructField('aux_data', T.ArrayType(T.IntegerType())),
            ])
        )

        df.write_ext.redis(
            key_by=['key_2'],
            max_pipeline_size=3,
            redis_client_init=partial(redis.StrictRedis, 'redis.docker'),
        )

        redis_client = redis.StrictRedis('redis.docker')

        self.assertEqual(redis_client.keys(), [b'k14'])

        written_data = json.loads(redis_client.get('k14'))
        expected = {'key_1': 'k1', 'key_2': 'k14', 'aux_data': [1, 14, 141]}
        self.assertEqual(written_data, expected)

    def test_invalid_input(self):
        df = self.spark.createDataFrame(
            data=[],
            schema=T.StructType([T.StructField('key_1', T.StringType())]),
        )

        with six.assertRaisesRegex(
                self,
                ValueError,
                'redis: expire must be positive',
        ):
            df.write_ext.redis(
                key_by=['key_1'],
                expire=0,
                host='redis.docker',
            )

        with six.assertRaisesRegex(
                self,
                ValueError,
                'redis: bzip2, gzip and zlib are the only supported compression codecs',
        ):
            df.write_ext.redis(
                key_by=['key_1'],
                compression='snappy',
                host='redis.docker',
            )

        with six.assertRaisesRegex(
                self,
                ValueError,
                'redis: max pipeline size must be positive',
        ):
            df.write_ext.redis(
                key_by=['key_1'],
                max_pipeline_size=0,
                host='redis.docker',
            )

        with six.assertRaisesRegex(
                self,
                ValueError,
                'redis: only append \(default\), ignore and overwrite modes are supported',
        ):
            df.write_ext.redis(
                key_by=['key_1'],
                mode='error',
                host='redis.docker',
            )

        with six.assertRaisesRegex(
                self,
                AssertionError,
                'redis: At least one of host or redis_client_init must be provided',
        ):
            df.write_ext.redis(
                key_by=['key_1'],
            )


class TestWriteRedisByURL(SparklyGlobalSessionTest):
    # similar to TestWriteRedis, but integrates URL parsing
    session = SparklyTestSession

    def tearDown(self):
        redis.StrictRedis('redis.docker', 6379).flushdb()
        super(TestWriteRedisByURL, self).tearDown()

    @staticmethod
    def _gzip_decompress(data):
        try:
            return gzip.decompress(data)
        except AttributeError:  #py27
            return zlib.decompress(data, zlib.MAX_WBITS | 16)

    def test_simple_key_uncompressed(self):
        df = self.spark.createDataFrame(
            data=[
                ('k1', 'k14', [1, 14, 141]),
                ('k1', 'k12', [1, 12, 121]),
                ('k1', 'k11', [1, 11, 111]),
                ('k1', 'k13', [1, 13, 131]),
            ],
            schema=T.StructType([
                T.StructField('key_1', T.StringType()),
                T.StructField('key_2', T.StringType()),
                T.StructField('aux_data', T.ArrayType(T.IntegerType())),
            ])
        )

        df.write_ext.by_url('redis://redis.docker?keyBy=key_2&maxPipelineSize=3')

        redis_client = redis.StrictRedis('redis.docker')

        self.assertRowsEqual(
            redis_client.keys(),
            [b'k11', b'k12', b'k13', b'k14'],
            ignore_order=True,
        )

        written_data = [
            json.loads(redis_client.get(key)) for key in ['k11', 'k12', 'k13', 'k14']
        ]

        expected = [
            {'key_1': 'k1', 'key_2': 'k11', 'aux_data': [1, 11, 111]},
            {'key_1': 'k1', 'key_2': 'k12', 'aux_data': [1, 12, 121]},
            {'key_1': 'k1', 'key_2': 'k13', 'aux_data': [1, 13, 131]},
            {'key_1': 'k1', 'key_2': 'k14', 'aux_data': [1, 14, 141]},
        ]

        self.assertEqual(written_data, expected)

    def test_composite_key_compressed(self):
        df = self.spark.createDataFrame(
            data=[
                ('k1', 'k14', [1, 14, 141]),
                ('k1', 'k12', [1, 12, 121]),
                ('k1', 'k11', [1, 11, 111]),
                ('k1', 'k13', [1, 13, 131]),
            ],
            schema=T.StructType([
                T.StructField('key_1', T.StringType()),
                T.StructField('key_2', T.StringType()),
                T.StructField('aux_data', T.ArrayType(T.IntegerType())),
            ])
        )

        df.write_ext.by_url(
            'redis://redis.docker?keyBy=key_1,key_2&compression=gzip&maxPipelineSize=3'
        )

        redis_client = redis.StrictRedis('redis.docker')

        self.assertRowsEqual(
            redis_client.keys(),
            [b'k1.k11', b'k1.k12', b'k1.k13', b'k1.k14'],
            ignore_order=True,
        )

        written_data = [
            json.loads(self._gzip_decompress(redis_client.get(key)))
            for key in ['k1.k11', 'k1.k12', 'k1.k13', 'k1.k14']
        ]

        expected = [
            {'key_1': 'k1', 'key_2': 'k11', 'aux_data': [1, 11, 111]},
            {'key_1': 'k1', 'key_2': 'k12', 'aux_data': [1, 12, 121]},
            {'key_1': 'k1', 'key_2': 'k13', 'aux_data': [1, 13, 131]},
            {'key_1': 'k1', 'key_2': 'k14', 'aux_data': [1, 14, 141]},
        ]

        self.assertEqual(written_data, expected)

    def test_composite_key_with_prefix_compressed(self):
        df = self.spark.createDataFrame(
            data=[
                ('k1', 'k14', [1, 14, 141]),
                ('k1', 'k12', [1, 12, 121]),
                ('k1', 'k11', [1, 11, 111]),
                ('k1', 'k13', [1, 13, 131]),
            ],
            schema=T.StructType([
                T.StructField('key_1', T.StringType()),
                T.StructField('key_2', T.StringType()),
                T.StructField('aux_data', T.ArrayType(T.IntegerType())),
            ])
        )

        df.write_ext.by_url(
            'redis://redis.docker?'
            'keyBy=key_1,key_2&'
            'keyPrefix=hello&'
            'compression=gzip&'
            'maxPipelineSize=3'
        )

        redis_client = redis.StrictRedis('redis.docker')

        self.assertRowsEqual(
            redis_client.keys(),
            [b'hello.k1.k11', b'hello.k1.k12', b'hello.k1.k13', b'hello.k1.k14'],
            ignore_order=True,
        )

        written_data = [
            json.loads(self._gzip_decompress(redis_client.get(key)))
            for key in ['hello.k1.k11', 'hello.k1.k12', 'hello.k1.k13', 'hello.k1.k14']
        ]

        expected = [
            {'key_1': 'k1', 'key_2': 'k11', 'aux_data': [1, 11, 111]},
            {'key_1': 'k1', 'key_2': 'k12', 'aux_data': [1, 12, 121]},
            {'key_1': 'k1', 'key_2': 'k13', 'aux_data': [1, 13, 131]},
            {'key_1': 'k1', 'key_2': 'k14', 'aux_data': [1, 14, 141]},
        ]

        self.assertEqual(written_data, expected)

    def test_group_by(self):
        df = self.spark.createDataFrame(
            data=[
                ('k4', 'k14', [1, 14, 141]),
                ('k1', 'k12', [1, 12, 121]),
                ('k1', 'k11', [1, 11, 111]),
                ('k1', 'k13', [1, 13, 131]),
            ],
            schema=T.StructType([
                T.StructField('key_1', T.StringType()),
                T.StructField('key_2', T.StringType()),
                T.StructField('aux_data', T.ArrayType(T.IntegerType())),
            ])
        )

        df.write_ext.by_url(
            'redis://redis.docker?keyBy=key_1&groupByKey=true&maxPipelineSize=2'
        )

        redis_client = redis.StrictRedis('redis.docker')

        self.assertRowsEqual(redis_client.keys(), [b'k1', b'k4'], ignore_order=True)

        written_data = [json.loads(redis_client.get(key)) for key in [b'k1', b'k4']]

        expected = [
            [
                {'key_1': 'k1', 'key_2': 'k11', 'aux_data': [1, 11, 111]},
                {'key_1': 'k1', 'key_2': 'k12', 'aux_data': [1, 12, 121]},
                {'key_1': 'k1', 'key_2': 'k13', 'aux_data': [1, 13, 131]},
            ],
            [{'key_1': 'k4', 'key_2': 'k14', 'aux_data': [1, 14, 141]}],
        ]

        self.assertRowsEqual(written_data, expected, ignore_order=True)


    def test_exclude_key_columns(self):
        df = self.spark.createDataFrame(
            data=[
                ('k1', 'k14', [1, 14, 141]),
                ('k1', 'k12', [1, 12, 121]),
                ('k1', 'k11', [1, 11, 111]),
                ('k1', 'k13', [1, 13, 131]),
            ],
            schema=T.StructType([
                T.StructField('key_1', T.StringType()),
                T.StructField('key_2', T.StringType()),
                T.StructField('aux_data', T.ArrayType(T.IntegerType())),
            ])
        )

        redis_client = redis.StrictRedis('redis.docker')

        # simple key
        df.write_ext.by_url(
            'redis://redis.docker?keyBy=key_2&keyPrefix=hello&excludeKeyColumns=true'
        )

        self.assertRowsEqual(
            redis_client.keys(),
            [b'hello.k11', b'hello.k12', b'hello.k13', b'hello.k14'],
            ignore_order=True,
        )

        written_data = [
            json.loads(redis_client.get(key))
            for key in ['hello.k11', 'hello.k12', 'hello.k13', 'hello.k14']
        ]

        expected = [
            {'key_1': 'k1', 'aux_data': [1, 11, 111]},
            {'key_1': 'k1', 'aux_data': [1, 12, 121]},
            {'key_1': 'k1', 'aux_data': [1, 13, 131]},
            {'key_1': 'k1', 'aux_data': [1, 14, 141]},
        ]

        self.assertEqual(written_data, expected)

        redis_client.flushdb()

        # composite key
        df.write_ext.by_url(
            'redis://redis.docker?keyBy=key_1,key_2&keyPrefix=hello&excludeKeyColumns=true'
        )

        self.assertRowsEqual(
            redis_client.keys(),
            [b'hello.k1.k11', b'hello.k1.k12', b'hello.k1.k13', b'hello.k1.k14'],
            ignore_order=True,
        )

        written_data = [
            json.loads(redis_client.get(key))
            for key in ['hello.k1.k11', 'hello.k1.k12', 'hello.k1.k13', 'hello.k1.k14']
        ]

        expected = [
            {'aux_data': [1, 11, 111]},
            {'aux_data': [1, 12, 121]},
            {'aux_data': [1, 13, 131]},
            {'aux_data': [1, 14, 141]},
        ]

        self.assertEqual(written_data, expected)

    def test_exclude_null_fields(self):
        df = self.spark.createDataFrame(
            data=[
                ('k1', 'k14', [1, 14, 141]),
                (None, 'k12', [1, 12, 121]),
                ('k1', 'k11', None),
                ('k1', 'k13', [1, 13, 131]),
            ],
            schema=T.StructType([
                T.StructField('key_1', T.StringType()),
                T.StructField('key_2', T.StringType()),
                T.StructField('aux_data', T.ArrayType(T.IntegerType())),
            ])
        )

        redis_client = redis.StrictRedis('redis.docker')

        df.write_ext.by_url(
            'redis://redis.docker?keyBy=key_2&keyPrefix=hello&excludeNullFields=true'
        )

        self.assertRowsEqual(
            redis_client.keys(),
            [b'hello.k11', b'hello.k12', b'hello.k13', b'hello.k14'],
            ignore_order=True,
        )

        written_data = [
            json.loads(redis_client.get(key))
            for key in ['hello.k11', 'hello.k12', 'hello.k13', 'hello.k14']
        ]

        expected = [
            {'key_1': 'k1', 'key_2': 'k11'},
            {'key_2': 'k12', 'aux_data': [1, 12, 121]},
            {'key_1': 'k1', 'key_2': 'k13', 'aux_data': [1, 13, 131]},
            {'key_1': 'k1', 'key_2': 'k14', 'aux_data': [1, 14, 141]},
        ]

        self.assertEqual(written_data, expected)


    def test_exclude_key_columns(self):
        df = self.spark.createDataFrame(
            data=[
                ('k1', 'k14', [1, 14, 141]),
                ('k1', 'k12', [1, 12, 121]),
                ('k1', 'k11', [1, 11, 111]),
                ('k1', 'k13', [1, 13, 131]),
            ],
            schema=T.StructType([
                T.StructField('key_1', T.StringType()),
                T.StructField('key_2', T.StringType()),
                T.StructField('aux_data', T.ArrayType(T.IntegerType())),
            ])
        )

        redis_client = redis.StrictRedis('redis.docker')

        # simple key
        df.write_ext.by_url(
            'redis://redis.docker?keyBy=key_2&keyPrefix=hello&excludeKeyColumns=true'
        )

        self.assertRowsEqual(
            redis_client.keys(),
            [b'hello.k11', b'hello.k12', b'hello.k13', b'hello.k14'],
            ignore_order=True,
        )

        written_data = [
            json.loads(redis_client.get(key))
            for key in ['hello.k11', 'hello.k12', 'hello.k13', 'hello.k14']
        ]

        expected = [
            {'key_1': 'k1', 'aux_data': [1, 11, 111]},
            {'key_1': 'k1', 'aux_data': [1, 12, 121]},
            {'key_1': 'k1', 'aux_data': [1, 13, 131]},
            {'key_1': 'k1', 'aux_data': [1, 14, 141]},
        ]

        self.assertEqual(written_data, expected)

        redis_client.flushdb()

        # composite key
        df.write_ext.by_url(
            'redis://redis.docker?keyBy=key_1,key_2&keyPrefix=hello&excludeKeyColumns=true'
        )

        self.assertRowsEqual(
            redis_client.keys(),
            [b'hello.k1.k11', b'hello.k1.k12', b'hello.k1.k13', b'hello.k1.k14'],
            ignore_order=True,
        )

        written_data = [
            json.loads(redis_client.get(key))
            for key in ['hello.k1.k11', 'hello.k1.k12', 'hello.k1.k13', 'hello.k1.k14']
        ]

        expected = [
            {'aux_data': [1, 11, 111]},
            {'aux_data': [1, 12, 121]},
            {'aux_data': [1, 13, 131]},
            {'aux_data': [1, 14, 141]},
        ]

        self.assertEqual(written_data, expected)

    def test_exclude_null_fields(self):
        df = self.spark.createDataFrame(
            data=[
                ('k1', 'k14', [1, 14, 141]),
                (None, 'k12', [1, 12, 121]),
                ('k1', 'k11', None),
                ('k1', 'k13', [1, 13, 131]),
            ],
            schema=T.StructType([
                T.StructField('key_1', T.StringType()),
                T.StructField('key_2', T.StringType()),
                T.StructField('aux_data', T.ArrayType(T.IntegerType())),
            ])
        )

        redis_client = redis.StrictRedis('redis.docker')

        df.write_ext.by_url(
            'redis://redis.docker?keyBy=key_2&keyPrefix=hello&excludeNullFields=true'
        )

        self.assertRowsEqual(
            redis_client.keys(),
            [b'hello.k11', b'hello.k12', b'hello.k13', b'hello.k14'],
            ignore_order=True,
        )

        written_data = [
            json.loads(redis_client.get(key))
            for key in ['hello.k11', 'hello.k12', 'hello.k13', 'hello.k14']
        ]

        expected = [
            {'key_1': 'k1', 'key_2': 'k11'},
            {'key_2': 'k12', 'aux_data': [1, 12, 121]},
            {'key_1': 'k1', 'key_2': 'k13', 'aux_data': [1, 13, 131]},
            {'key_1': 'k1', 'key_2': 'k14', 'aux_data': [1, 14, 141]},
        ]

        self.assertEqual(written_data, expected)

    def test_expiration(self):
        df = self.spark.createDataFrame(
            data=[
                ('k1', 'k14', [1, 14, 141]),
                ('k1', 'k12', [1, 12, 121]),
                ('k1', 'k11', [1, 11, 111]),
                ('k1', 'k13', [1, 13, 131]),
            ],
            schema=T.StructType([
                T.StructField('key_1', T.StringType()),
                T.StructField('key_2', T.StringType()),
                T.StructField('aux_data', T.ArrayType(T.IntegerType())),
            ])
        )

        df.write_ext.by_url('redis://redis.docker?keyBy=key_2&keyPrefix=hello&expire=2')

        redis_client = redis.StrictRedis('redis.docker')

        self.assertRowsEqual(
            redis_client.keys(),
            [b'hello.k11', b'hello.k12', b'hello.k13', b'hello.k14'],
            ignore_order=True,
        )

        written_data = [
            json.loads(redis_client.get(key))
            for key in ['hello.k11', 'hello.k12', 'hello.k13', 'hello.k14']
        ]

        expected = [
            {'key_1': 'k1', 'key_2': 'k11', 'aux_data': [1, 11, 111]},
            {'key_1': 'k1', 'key_2': 'k12', 'aux_data': [1, 12, 121]},
            {'key_1': 'k1', 'key_2': 'k13', 'aux_data': [1, 13, 131]},
            {'key_1': 'k1', 'key_2': 'k14', 'aux_data': [1, 14, 141]},
        ]

        self.assertEqual(written_data, expected)

        sleep(3)

        self.assertEqual(redis_client.keys(), [])

    def test_mode(self):
        redis_client = redis.StrictRedis('redis.docker')
        redis_client.set('k11', '"hey!"')
        redis_client.set('k13', '"you!"')
        redis_client.set('k14', '"brick!"')

        df = self.spark.createDataFrame(
            data=[
                ('k1', 'k14', [1, 14, 141]),
                ('k1', 'k12', [1, 12, 121]),
                ('k1', 'k11', [1, 11, 111]),
                ('k1', 'k13', [1, 13, 131]),
            ],
            schema=T.StructType([
                T.StructField('key_1', T.StringType()),
                T.StructField('key_2', T.StringType()),
                T.StructField('aux_data', T.ArrayType(T.IntegerType())),
            ])
        )

        # test ignore
        df.write_ext.by_url('redis://redis.docker?keyBy=key_2&mode=ignore')

        self.assertRowsEqual(
            redis_client.keys(),
            [b'k11', b'k12', b'k13', b'k14'],
            ignore_order=True,
        )

        written_data = [
            json.loads(redis_client.get(key)) for key in ['k11', 'k12', 'k13', 'k14']
        ]

        expected = [
            'hey!',
            {'key_1': 'k1', 'key_2': 'k12', 'aux_data': [1, 12, 121]},
            'you!',
            'brick!',
        ]

        self.assertEqual(written_data, expected)

        # test append
        df.write_ext.by_url('redis://redis.docker?keyBy=key_2&mode=append')

        self.assertRowsEqual(
            redis_client.keys(),
            [b'k11', b'k12', b'k13', b'k14'],
            ignore_order=True,
        )

        written_data = [
            json.loads(redis_client.get(key)) for key in ['k11', 'k12', 'k13', 'k14']
        ]

        expected = [
            {'key_1': 'k1', 'key_2': 'k11', 'aux_data': [1, 11, 111]},
            {'key_1': 'k1', 'key_2': 'k12', 'aux_data': [1, 12, 121]},
            {'key_1': 'k1', 'key_2': 'k13', 'aux_data': [1, 13, 131]},
            {'key_1': 'k1', 'key_2': 'k14', 'aux_data': [1, 14, 141]},
        ]

        self.assertEqual(written_data, expected)

        # test overwrite
        df.where(F.col('key_2') == 'k11').write_ext.by_url(
            'redis://redis.docker?keyBy=key_2&mode=overwrite'
        )

        self.assertEqual(redis_client.keys(), [b'k11'])

        written_data = [json.loads(redis_client.get('k11'))]

        expected = [
            {'key_1': 'k1', 'key_2': 'k11', 'aux_data': [1, 11, 111]},
        ]

        self.assertEqual(written_data, expected)

    def test_db(self):
        df = self.spark.createDataFrame(
            data=[
                ('k1', 'k14', [1, 14, 141]),
            ],
            schema=T.StructType([
                T.StructField('key_1', T.StringType()),
                T.StructField('key_2', T.StringType()),
                T.StructField('aux_data', T.ArrayType(T.IntegerType())),
            ])
        )

        df.write_ext.by_url('redis://redis.docker/1?keyBy=key_2&maxPipelineSize=3')

        redis_client = redis.StrictRedis('redis.docker', db=1)

        self.assertEqual(redis_client.keys(), [b'k14'])

        written_data = json.loads(redis_client.get('k14'))
        expected = {'key_1': 'k1', 'key_2': 'k14', 'aux_data': [1, 14, 141]}
        self.assertEqual(written_data, expected)

    def test_invalid_input(self):
        df = self.spark.createDataFrame(
            data=[],
            schema=T.StructType([T.StructField('key_1', T.StringType())]),
        )

        with six.assertRaisesRegex(
                self,
                AssertionError,
                'redis: url must define keyBy columns to construct redis key',
        ):
            df.write_ext.by_url('redis://redis.docker')

        with six.assertRaisesRegex(
                self,
                ValueError,
                'redis: true and false \(default\) are the only supported groupByKey values',
        ):
            df.write_ext.by_url('redis://redis.docker?keyBy=key_1&groupByKey=tru')

        with six.assertRaisesRegex(
                self,
                ValueError,
                'redis: true and false \(default\) are the only supported excludeKeyColumns '
                'values',
        ):
            df.write_ext.by_url('redis://redis.docker?keyBy=key_1&excludeKeyColumns=tru')

        with six.assertRaisesRegex(
                self,
                ValueError,
                'redis: expire must be positive',
        ):
            df.write_ext.by_url('redis://redis.docker?keyBy=key_1&expire=0')

        with six.assertRaisesRegex(
                self,
                ValueError,
                'redis: expire must be a base 10, positive integer',
        ):
            df.write_ext.by_url('redis://redis.docker?keyBy=key_1&expire=0x11')

        with six.assertRaisesRegex(
                self,
                ValueError,
                'redis: bzip2, gzip and zlib are the only supported compression codecs',
        ):
            df.write_ext.by_url('redis://redis.docker?keyBy=key_1&compression=snappy')

        with six.assertRaisesRegex(
                self,
                ValueError,
                'redis: max pipeline size must be positive',
        ):
            df.write_ext.by_url('redis://redis.docker?keyBy=key_1&maxPipelineSize=0')

        with six.assertRaisesRegex(
                self,
                ValueError,
                'redis: maxPipelineSize must be a base 10, positive integer',
        ):
            df.write_ext.by_url('redis://redis.docker?keyBy=key_1&maxPipelineSize=0x11')

        with six.assertRaisesRegex(
                self,
                ValueError,
                'redis: only append \(default\), ignore and overwrite modes are supported',
        ):
            df.write_ext.by_url('redis://redis.docker?keyBy=key_1&mode=error')
