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

from sparkly.testing import (
    CassandraFixture,
    ElasticFixture,
    MysqlFixture,
    SparklyGlobalSessionTest,
    KafkaFixture)
from sparkly.utils import absolute_path
from tests.integration.base import SparklyTestSession

try:
    from kafka import KafkaConsumer
except ImportError:
    pass


class TestAssertions(SparklyGlobalSessionTest):
    session = SparklyTestSession

    def test_assert_dataframe_equal(self):
        df = self.spark.createDataFrame([('Alice', 1),
                                         ('Kelly', 1),
                                         ('BigBoss', 999)],
                                        ['name', 'age'])
        self.assertDataFrameEqual(
            df,
            [{'name': 'Alice', 'age': 1},
             {'name': 'BigBoss', 'age': 999},
             {'name': 'Kelly', 'age': 1},
             ],
            ordered=False,
        )

        with self.assertRaises(AssertionError):
            self.assertDataFrameEqual(
                df,
                [{'name': 'Alice', 'age': 1},
                 {'name': 'BigBoss', 'age': 999},
                 {'name': 'Kelly', 'age': 1},
                 ],
                ordered=True,
            )


class TestCassandraFixtures(SparklyGlobalSessionTest):
    session = SparklyTestSession

    def test_cassandra_fixture(self):
        data_in_cassandra = CassandraFixture(
            'cassandra.docker',
            absolute_path(__file__, 'resources', 'test_fixtures', 'cassandra_setup.cql'),
            absolute_path(__file__, 'resources', 'test_fixtures', 'cassandra_teardown.cql'),
        )

        with data_in_cassandra:
            df = self.spark.read_ext.by_url('cassandra://cassandra.docker/sparkly_test/test')
            self.assertDataFrameEqual(df, [
                {
                    'uid': '1',
                    'countries': {'AE': 13, 'BE': 1, 'BH': 3, 'CA': 1, 'DZ': 1, 'EG': 206},
                },
            ], fields=['uid', 'countries'])


class TestMysqlFixtures(SparklyGlobalSessionTest):

    session = SparklyTestSession

    fixtures = [
        MysqlFixture(
            'mysql.docker',
            'root',
            None,
            absolute_path(__file__, 'resources', 'test_fixtures', 'mysql_setup.sql'),
            absolute_path(__file__, 'resources', 'test_fixtures', 'mysql_teardown.sql'),
        )
    ]

    def test_mysql_fixture(self):
        df = self.spark.read_ext.by_url('mysql://mysql.docker/sparkly_test/test?user=root&password=')
        self.assertDataFrameEqual(df, [
            {'id': 1, 'name': 'john', 'surname': 'sk', 'age': 111},
        ])


class TestElasticFixture(SparklyGlobalSessionTest):

    session = SparklyTestSession

    class_fixtures = [
        ElasticFixture(
            'elastic.docker',
            'sparkly_test_fixture',
            'test',
            absolute_path(__file__, 'resources', 'test_fixtures', 'mapping.json'),
            absolute_path(__file__, 'resources', 'test_fixtures', 'data.json'),
        )
    ]

    def test_elastic_fixture(self):
        df = self.spark.read_ext.by_url('elastic://elastic.docker/sparkly_test_fixture/test?'
                                     'es.read.metadata=false')
        self.assertDataFrameEqual(df, [
            {'name': 'John', 'age': 56},
        ])


class TestKafkaFixture(SparklyGlobalSessionTest):

    session = SparklyTestSession

    topic = 'sparkly.test.fixture.{}'.format(uuid.uuid4().hex[:10])
    fixtures = [
        KafkaFixture(
            'kafka.docker',
            topic=topic,
            key_serializer=lambda item: json.dumps(item).encode('utf-8'),
            value_serializer=lambda item: json.dumps(item).encode('utf-8'),
            data=absolute_path(__file__, 'resources', 'test_fixtures', 'kafka.json'),
        )
    ]

    def test_kafka_fixture(self):
        consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers='kafka.docker:9092',
            key_deserializer=lambda item: json.loads(item.decode('utf-8')),
            value_deserializer=lambda item: json.loads(item.decode('utf-8')),
            auto_offset_reset='earliest',
        )

        actual_data = []
        for i in range(5):
            message = next(consumer)
            data = {'key': message.key, 'value': message.value}
            actual_data.append(data)

        expected_data = self.spark.read.json(
            absolute_path(__file__, 'resources', 'test_fixtures', 'kafka.json')
        )
        self.assertDataFrameEqual(expected_data, actual_data)
