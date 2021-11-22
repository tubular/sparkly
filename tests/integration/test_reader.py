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

from sparkly.exceptions import InvalidArgumentError
from sparkly.testing import (
    SparklyGlobalSessionTest,
    SparklyTest,
    CassandraFixture,
    MysqlFixture,
    ElasticFixture,
    KafkaFixture,
)
from sparkly.utils import absolute_path, kafka_get_topics_offsets
from tests.integration.base import (
    SparklyTestSession,
)


class SparklyReaderCassandraTest(SparklyGlobalSessionTest):
    session = SparklyTestSession

    fixtures = [
        CassandraFixture(
            'cassandra.docker',
            absolute_path(__file__, 'resources', 'test_read', 'cassandra_setup.cql'),
            absolute_path(__file__, 'resources', 'test_read', 'cassandra_teardown.cql'),
        )
    ]

    def test_read(self):
        df = self.spark.read_ext.cassandra(
            host='cassandra.docker',
            port=9042,
            keyspace='sparkly_test',
            table='test',
            consistency='ONE',
        )

        self.assertDataFrameEqual(df, [
            {
                'countries': {'DZ': 1, 'EG': 206, 'BE': 1, 'CA': 1, 'AE': 13, 'BH': 3},
                'uid': '1',
                'created': '1234567894',
            },
            {
                'countries': {'DZ': 1, 'EG': 206, 'BE': 1, 'CA': 1, 'AE': 13, 'BH': 3},
                'uid': '2',
                'created': '1234567893',
            },
            {
                'countries': {'DZ': 1, 'EG': 206, 'BE': 1, 'CA': 1, 'AE': 13, 'BH': 3},
                'uid': '3',
                'created': '1234567891',
            }
        ])


ELASTIC_TEST_DATA = [
    {
        'name': 'Smith3',
        'topics': [1, 4, 5],
        'age': 31,
        'demo': {
            'age_30': 110,
            'age_10': 50,
        }
    },
    {
        'name': 'Smith4',
        'topics': [4, 5],
        'age': 12,
        'demo': {
            'age_30': 20,
            'age_10': 1,
        }
    }
]


class SparklyReaderElasticTest(SparklyGlobalSessionTest):
    session = SparklyTestSession

    fixtures = [
        ElasticFixture(
            'elastic.docker',
            'sparkly_test',
            None,
            None,
            absolute_path(__file__, 'resources', 'test_read', 'elastic7_setup.json'),
        )
    ]

    def test_elastic(self):
        df = self.spark.read_ext.elastic(
            host='elastic.docker',
            port=9200,
            es_index='sparkly_test',
            es_type=None,
            query='?q=name:*Smith*',
            options={
                'es.read.field.as.array.include': 'topics',
                'es.read.metadata': 'false',
            },
        )

        self.assertDataFrameEqual(df, ELASTIC_TEST_DATA)


class SparklyReaderMySQLTest(SparklyGlobalSessionTest):
    session = SparklyTestSession

    fixtures = [
        MysqlFixture(
            'mysql.docker',
            'root',
            None,
            absolute_path(__file__, 'resources', 'test_read', 'mysql_setup.sql'),
            absolute_path(__file__, 'resources', 'test_read', 'mysql_teardown.sql'),
        )
    ]

    def test_read_mysql(self):
        df = self.spark.read_ext.mysql(
            host='mysql.docker',
            database='sparkly_test',
            table='test',
            options={
                'user': 'root',
                'password': '',
            }
        )

        self.assertDataFrameEqual(df, [
            {'id': 1, 'name': 'john', 'surname': 'sk', 'age': 111},
            {'id': 2, 'name': 'john', 'surname': 'po', 'age': 222},
            {'id': 3, 'name': 'john', 'surname': 'ku', 'age': 333},
        ])


class TestReaderKafka(SparklyGlobalSessionTest):
    session = SparklyTestSession

    def setUp(self):
        self.json_decoder = lambda item: json.loads(item.decode('utf-8'))
        self.json_encoder = lambda item: json.dumps(item).encode('utf-8')
        self.topic = 'test.topic.write.kafka.{}'.format(uuid.uuid4().hex[:10])
        self.fixture_path = absolute_path(__file__, 'resources', 'test_read', 'kafka_setup.json')
        self.fixture = KafkaFixture(
            'kafka.docker',
            topic=self.topic,
            key_serializer=self.json_encoder,
            value_serializer=self.json_encoder,
            data=self.fixture_path,
        )
        self.fixture.setup_data()
        self.expected_data_df = self.spark.read.json(self.fixture_path)
        self.expected_data = [item.asDict(recursive=True)
                              for item in self.expected_data_df.collect()]

    def test_read_by_topic(self):
        df = self.spark.read_ext.kafka(
            'kafka.docker',
            topic=self.topic,
            key_deserializer=self.json_decoder,
            value_deserializer=self.json_decoder,
            schema=self.expected_data_df.schema,
        )
        self.assertDataFrameEqual(
            df,
            self.expected_data,
        )

    def test_read_by_offsets(self):
        offsets = kafka_get_topics_offsets('kafka.docker', self.topic)
        df = self.spark.read_ext.kafka(
            'kafka.docker',
            topic=self.topic,
            offset_ranges=offsets,
            key_deserializer=self.json_decoder,
            value_deserializer=self.json_decoder,
            schema=self.expected_data_df.schema,
        )

        self.assertDataFrameEqual(df, self.expected_data)

        self.fixture.setup_data()

        offsets = kafka_get_topics_offsets('kafka.docker', self.topic)
        df = self.spark.read_ext.kafka(
            'kafka.docker',
            topic=self.topic,
            offset_ranges=offsets,
            key_deserializer=self.json_decoder,
            value_deserializer=self.json_decoder,
            schema=self.expected_data_df.schema,
        )

        self.assertDataFrameEqual(df, self.expected_data * 2)

        df = self.spark.read_ext.kafka(
            'kafka.docker',
            topic=self.topic,
            offset_ranges=offsets,
            key_deserializer=self.json_decoder,
            value_deserializer=self.json_decoder,
            schema=self.expected_data_df.schema,
            include_meta_cols=True,
        )
        expected = [
            # normal fields:
            'key',
            'value',
            # meta fields:
            'topic',
            'partition',
            'offset',
            'timestamp',
            'timestampType',
        ]
        self.assertListEqual(sorted(expected), sorted(df.schema.fieldNames()))

    def test_argument_errors(self):
        with self.assertRaises(InvalidArgumentError):
            self.spark.read_ext.kafka(
                'kafka.docker',
                topic=self.topic,
                key_deserializer=self.json_decoder,
                value_deserializer=self.json_decoder,
                # no schema!
            )
            self.spark.read_ext.kafka(
                'kafka.docker',
                topic=self.topic,
                key_deserializer=self.json_decoder,
                # no schema!
            )
            self.spark.read_ext.kafka(
                'kafka.docker',
                topic=self.topic,
                value_deserializer=self.json_decoder,
                # no schema!
            )
