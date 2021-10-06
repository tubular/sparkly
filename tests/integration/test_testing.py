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
import pickle
import unittest

from sparkly.session import SparklySession
from sparkly.testing import (
    CassandraFixture,
    ElasticFixture,
    MysqlFixture,
    SparklyGlobalSessionTest,
    SparklyTest,
    KafkaFixture,
    KafkaWatcher,
)
from sparkly.utils import absolute_path
from tests.integration.base import (
    SparklyTestSession,
)


try:
    from kafka import KafkaConsumer, KafkaProducer
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


class TestSparklyGlobalSessionTest(unittest.TestCase):

    def test_imports_test_target(self):

        class MyGlobalTest(SparklyGlobalSessionTest):
            session = SparklyTestSession
            test_target = 'tests.integration.fake_modules.testing.is_fake'


        MyGlobalTest.setUpClass()

        self.assertTrue(is_fake)


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
            None,
            absolute_path(__file__, 'resources', 'test_fixtures', 'mapping.json'),
            absolute_path(__file__, 'resources', 'test_fixtures', 'data_for_es7.json'),
        )
    ]

    def test_elastic_fixture(self):
        df = self.spark.read_ext.by_url(
            'elastic://elastic.docker/sparkly_test_fixture?es.read.metadata=false'
        )
        self.assertDataFrameEqual(df, [{'name': 'John', 'age': 56}])


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


class TestKafkaWatcher(SparklyGlobalSessionTest):
    session = SparklyTestSession

    def test_write_kafka_dataframe(self):
        host = 'kafka.docker'
        topic = 'test.topic.kafkawatcher.{}'.format(uuid.uuid4().hex[:10])
        port = 9092
        input_df, expected_data = self.get_test_data('kafka_watcher_1.json')

        kafka_watcher = KafkaWatcher(
            self.spark,
            input_df.schema,
            pickle.loads,
            pickle.loads,
            host,
            topic,
            port,
        )
        with kafka_watcher:
            expected_count = self.write_data(input_df, host, topic, port)
        self.assertEqual(kafka_watcher.count, expected_count)
        self.assertDataFrameEqual(kafka_watcher.df, expected_data)

        with kafka_watcher:
            pass
        self.assertEqual(kafka_watcher.count, 0)
        self.assertIsNone(kafka_watcher.df, None)

        input_df, expected_data = self.get_test_data('kafka_watcher_2.json')
        with kafka_watcher:
            expected_count = self.write_data(input_df, host, topic, port)
        self.assertEqual(kafka_watcher.count, expected_count)
        self.assertDataFrameEqual(kafka_watcher.df, expected_data)

    def get_test_data(self, filename):
        file_path = absolute_path(__file__, 'resources', 'test_testing', filename)
        df = self.spark.read.json(file_path)
        data = [item.asDict(recursive=True) for item in df.collect()]
        return df, data

    def write_data(self, df, host, topic, port):
        producer = KafkaProducer(
            bootstrap_servers=['{}:{}'.format(host, port)],
            key_serializer=pickle.dumps,
            value_serializer=pickle.dumps,
        )
        rows = df.collect()
        for row in rows:
            producer.send(topic, key=row.key, value=row.value)
        producer.flush()
        return len(rows)


class TestSwitchingBetweenTestSessions(unittest.TestCase):
    # Test whether a user can switch between different sessions
    # during tests

    def test_switch_session_between_sparkly_tests(self):
        # Define a test session with an ES6 dependency
        class SessionA(SparklySession):
            packages = [
                'org.elasticsearch:elasticsearch-spark-20_2.11:6.5.4',
            ]

            repositories = [
                'http://packages.confluent.io/maven/',
            ]

        class TestSessionA(SparklyTest):
            session = SessionA

        # Define a test session with an ES7 dependency
        class SessionB(SparklySession):
            packages = [
                'org.elasticsearch:elasticsearch-spark-20_2.11:7.3.1',
            ]

            repositories = [
                'http://packages.confluent.io/maven/',
            ]

        class TestSessionB(SparklyTest):
            session = SessionB

        # Make sure that when the ES6 session is set up, the underlying
        # spark session contains the appropriate jars
        TestSessionA.setUpClass()
        expected_jars = [
            'file:///root/.ivy2/jars/org.elasticsearch_elasticsearch-spark-20_2.11-6.5.4.jar',
        ]
        installed_jars = list(TestSessionA.spark._jsc.jars())
        self.assertEqual(installed_jars, expected_jars)
        TestSessionA.tearDownClass()

        # And now make sure that when the ES7 session is set up, the underlying
        # spark session contains the appropriate jars as well
        TestSessionB.setUpClass()
        expected_jars = [
            'file:///root/.ivy2/jars/org.elasticsearch_elasticsearch-spark-20_2.11-7.3.1.jar',
        ]
        installed_jars = list(TestSessionB.spark._jsc.jars())
        self.assertEqual(installed_jars, expected_jars)
        TestSessionB.tearDownClass()

    def test_switch_global_session_between_sparkly_tests(self):
        # Define a test session with an ES6 dependency
        class SessionA(SparklySession):
            packages = [
                'org.elasticsearch:elasticsearch-spark-20_2.11:6.5.4',
            ]

            repositories = [
                'http://packages.confluent.io/maven/',
            ]

        class TestSessionA(SparklyGlobalSessionTest):
            session = SessionA

        # Define a test session with an ES7 dependency
        class SessionB(SparklySession):
            packages = [
                'org.elasticsearch:elasticsearch-spark-20_2.11:7.3.1',
            ]

            repositories = [
                'http://packages.confluent.io/maven/',
            ]

        class TestSessionB(SparklyGlobalSessionTest):
            session = SessionB

        # Make sure that when the ES6 session is set up, the underlying
        # spark session contains the appropriate jars
        TestSessionA.setUpClass()
        expected_jars = [
            'file:///root/.ivy2/jars/org.elasticsearch_elasticsearch-spark-20_2.11-6.5.4.jar',
        ]
        installed_jars = list(TestSessionA.spark._jsc.jars())
        self.assertEqual(installed_jars, expected_jars)
        TestSessionA.tearDownClass()

        # And now make sure that when the ES7 session is set up, the underlying
        # spark session contains the appropriate jars as well
        TestSessionB.setUpClass()
        expected_jars = [
            'file:///root/.ivy2/jars/org.elasticsearch_elasticsearch-spark-20_2.11-7.3.1.jar',
        ]
        installed_jars = list(TestSessionB.spark._jsc.jars())
        self.assertEqual(installed_jars, expected_jars)
        TestSessionB.tearDownClass()
