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
import uuid
from shutil import rmtree
from tempfile import mkdtemp

from sparkly.utils import absolute_path
from sparkly.testing import (
    SparklyGlobalSessionTest,
    CassandraFixture,
    ElasticFixture,
    MysqlFixture,
)
from tests.integration.base import SparklyTestSession

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
        )

        written_df = self.spark.read_ext.by_url(
            'cassandra://cassandra.docker/'
            'sparkly_test/test_writer'
            '?consistency=ONE'
        )
        self.assertDataFrameEqual(written_df, TEST_DATA)


class TestWriteElastic(SparklyGlobalSessionTest):
    session = SparklyTestSession

    fixtures = [
        ElasticFixture(
            'elastic.docker',
            'sparkly_test',
            'test',
            None,
            absolute_path(__file__, 'resources', 'test_write', 'elastic_setup.json')
        )
    ]

    def test_write_elastic(self):
        df = self.spark.createDataFrame(TEST_DATA)

        df.write_ext.elastic(
            host='elastic.docker',
            port=9200,
            es_index='sparkly_test',
            es_type='test_writer',
            mode='overwrite',
            options={
                'es.mapping.id': 'uid',
            }
        )

        df = self.spark.read_ext.by_url(
            'elastic://elastic.docker/sparkly_test/test_writer?es.read.metadata=false'
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
