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
import logging
import sys
import tempfile
from unittest import TestCase

from sparkly import SparklySession
from sparkly.exceptions import FixtureError
from sparkly.utils import kafka_get_topics_offsets

if sys.version_info.major == 3:
    from http.client import HTTPConnection
else:
    from httplib import HTTPConnection

try:
    from cassandra.cluster import Cluster
    CASSANDRA_FIXTURES_SUPPORT = True
except ImportError:
    CASSANDRA_FIXTURES_SUPPORT = False

try:
    import pymysql as connector
    MYSQL_FIXTURES_SUPPORT = True
except ImportError:
    try:
        import mysql.connector as connector
        MYSQL_FIXTURES_SUPPORT = True
    except ImportError:
        MYSQL_FIXTURES_SUPPORT = False

try:
    from kafka import KafkaProducer, SimpleClient
    KAFKA_FIXTURES_SUPPORT = True
except ImportError:
    KAFKA_FIXTURES_SUPPORT = False


logger = logging.getLogger()


_test_session_cache = None


class SparklyTest(TestCase):
    """Base test for spark scrip tests.

    Initialize and shut down Session specified in `session` attribute.

    Example:

        >>> class MyTestCase(SparklyTest):
        ...     def test(self):
        ...         self.assertDataFrameEqual(
        ...              self.spark.sql('SELECT 1 as one').collect(),
        ...              [{'one': 1}],
        ...         )
    """
    session = SparklySession

    class_fixtures = []
    fixtures = []
    maxDiff = None

    @classmethod
    def setup_session(cls):
        return cls.session({
            # Use in-memory hive metastore (faster tests).
            'spark.hadoop.javax.jdo.option.ConnectionURL':
                'jdbc:derby:memory:databaseName=metastore_db;create=true',
            'spark.hadoop.javax.jdo.option.ConnectionDriverName':
                'org.apache.derby.jdbc.EmbeddedDriver',

            # Isolate the warehouse inside of a random temporary directory (no side effects).
            'spark.sql.warehouse.dir': tempfile.mkdtemp(suffix='sparkly'),

            # Reduce number of shuffle partitions (faster tests).
            'spark.sql.shuffle.partitions': 4,
        })

    @classmethod
    def setUpClass(cls):
        super(SparklyTest, cls).setUpClass()

        # In case if project has a mix of SparklyTest and SparklyGlobalContextTest-based tests
        global _test_session_cache
        if _test_session_cache:
            logger.info('Found a global session, stopping it %r', _test_session_cache)
            _test_session_cache.stop()
            _test_session_cache = None

        cls.spark = cls.setup_session()

        for fixture in cls.class_fixtures:
            fixture.setup_data()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        super(SparklyTest, cls).tearDownClass()

        for fixture in cls.class_fixtures:
            fixture.teardown_data()

    def setUp(self):
        for fixture in self.fixtures:
            fixture.setup_data()

    def tearDown(self):
        for fixture in self.fixtures:
            fixture.teardown_data()

    def assertDataFrameEqual(self, actual_df, expected_data, fields=None, ordered=False):
        """Ensure that DataFrame has the right data inside.

        Args:
            actual_df (pyspark.sql.DataFrame|list[pyspark.sql.Row]): Dataframe to test data in.
            expected_data (list[dict]): Expected dataframe rows defined as dicts.
            fields (list[str]): Compare only certain fields.
            ordered (bool): Does order of rows matter?
        """
        if fields:
            actual_df = actual_df.select(*fields)

        actual_rows = actual_df.collect() if hasattr(actual_df, 'collect') else actual_df
        actual_data = [row.asDict(recursive=True) for row in actual_rows]

        if ordered:
            self.assertEqual(actual_data, expected_data)
        else:
            try:
                self.assertCountEqual(actual_data, expected_data)
            except AttributeError:
                self.assertItemsEqual(actual_data, expected_data)


class SparklyGlobalSessionTest(SparklyTest):
    """Base test case that keeps a single instance for the given session class across all tests.

    Integration tests are slow, especially when you have to start/stop Spark context
    for each test case. This class allows you to reuse Spark session across multiple test cases.
    """
    @classmethod
    def setUpClass(cls):
        global _test_session_cache

        if _test_session_cache and cls.session == type(_test_session_cache):
            logger.info('Reusing the global session for %r', cls.session)
            spark = _test_session_cache
        else:
            if _test_session_cache:
                logger.info('Stopping the previous global session %r', _test_session_cache)
                _test_session_cache.stop()

            logger.info('Starting the new global session for %r', cls.session)
            spark = _test_session_cache = cls.setup_session()

        cls.spark = spark

        for fixture in cls.class_fixtures:
            fixture.setup_data()

    @classmethod
    def tearDownClass(cls):
        cls.spark.catalog.clearCache()

        for fixture in cls.class_fixtures:
            fixture.teardown_data()


class Fixture(object):
    """Base class for fixtures.

    Fixture is a term borrowed from Django tests,
    it's data loaded into database for integration testing.
    """

    def setup_data(self):
        """Method called to load data into database."""
        raise NotImplementedError()

    def teardown_data(self):
        """Method called to remove data from database which was loaded by `setup_data`."""
        raise NotImplementedError()

    def __enter__(self):
        self.setup_data()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.teardown_data()

    @classmethod
    def read_file(cls, path):
        with open(path) as f:
            data = f.read()
        return data


class CassandraFixture(Fixture):
    """Fixture to load data into cassandra.

    Notes:
        * Depends on cassandra-driver.

    Examples:

           >>> class MyTestCase(SparklyTest):
           ...      fixtures = [
           ...          CassandraFixture(
           ...              'cassandra.host',
           ...              absolute_path(__file__, 'resources', 'setup.cql'),
           ...              absolute_path(__file__, 'resources', 'teardown.cql'),
           ...          )
           ...      ]
           ...

           >>> class MyTestCase(SparklyTest):
           ...      data = CassandraFixture(
           ...          'cassandra.host',
           ...          absolute_path(__file__, 'resources', 'setup.cql'),
           ...          absolute_path(__file__, 'resources', 'teardown.cql'),
           ...      )
           ...      def setUp(self):
           ...          data.setup_data()
           ...      def tearDown(self):
           ...          data.teardown_data()
           ...

           >>> def test():
           ...     fixture = CassandraFixture(...)
           ...     with fixture:
           ...        test_stuff()
           ...
    """

    def __init__(self, host, setup_file, teardown_file):
        if not CASSANDRA_FIXTURES_SUPPORT:
            raise NotImplementedError('cassandra-driver package isn\'t available. '
                                      'Use pip install sparkly[test] to fix it.')
        self.host = host
        self.setup_file = setup_file
        self.teardown_file = teardown_file

    def _execute(self, statements):
        cluster = Cluster([self.host])
        session = cluster.connect()
        for statement in statements.split(';'):
            if bool(statement.strip()):
                session.execute(statement.strip())

    def setup_data(self):
        self._execute(self.read_file(self.setup_file))

    def teardown_data(self):
        self._execute(self.read_file(self.teardown_file))


class ElasticFixture(Fixture):
    """Fixture for elastic integration tests.

    Examples:

           >>> class MyTestCase(SparklyTest):
           ...      fixtures = [
           ...          ElasticFixture(
           ...              'elastic.host',
           ...              'es_index',
           ...              'es_type',
           ...              '/path/to/mapping.json',
           ...              '/path/to/data.json',
           ...          )
           ...      ]
           ...
    """

    def __init__(self, host, es_index, es_type, mapping=None, data=None, port=None):
        self.host = host
        self.port = port or 9200
        self.es_index = es_index
        self.es_type = es_type
        self.mapping = mapping
        self.data = data

    def setup_data(self):
        if self.mapping:
            self._request(
                'PUT',
                '/{}'.format(self.es_index),
                json.dumps({
                    'settings': {
                        'index': {
                            'number_of_shards': 1,
                            'number_of_replicas': 1,
                        }
                    }
                }),
            )
            self._request(
                'PUT',
                '/{}/_mapping/{}'.format(self.es_index, self.es_type),
                self.read_file(self.mapping),
            )

        if self.data:
            self._request(
                'POST',
                '/_bulk',
                self.read_file(self.data),
            )
            self._request(
                'POST',
                '/_refresh',
            )

    def teardown_data(self):
        self._request(
            'DELETE',
            '/{}'.format(self.es_index),
        )

    def _request(self, method, url, body=None):
        connection = HTTPConnection(self.host, port=self.port)
        connection.request(method, url, body)
        response = connection.getresponse()
        if sys.version_info.major == 3:
            code = response.code
        else:
            code = response.status

        if code != 200:
            raise FixtureError('{}: {}'.format(code, response.read()))


class MysqlFixture(Fixture):
    """Fixture for mysql integration tests.

    Notes:
        * depends on PyMySql lib.

    Examples:

           >>> class MyTestCase(SparklyTest):
           ...      fixtures = [
           ...          MysqlFixture('mysql.host', 'user', 'password', '/path/to/data.sql')
           ...      ]
           ...      def test(self):
           ...          pass
           ...
    """

    def __init__(self, host, user, password=None, data=None, teardown=None):
        if not MYSQL_FIXTURES_SUPPORT:
            raise NotImplementedError('PyMySQL package isn\'t available. '
                                      'Use pip install sparkly[test] to fix it.')
        self.host = host
        self.user = user
        self.password = password
        self.data = data
        self.teardown = teardown

    def _execute(self, statements):
        ctx = connector.connect(
            user=self.user,
            password=self.password,
            host=self.host,
        )
        cursor = ctx.cursor()
        cursor.execute(statements)
        ctx.commit()
        cursor.close()
        ctx.close()

    def setup_data(self):
        self._execute(self.read_file(self.data))

    def teardown_data(self):
        self._execute(self.read_file(self.teardown))


class KafkaFixture(Fixture):
    """Fixture for kafka integration tests.

    Notes:
        * depends on kafka-python lib.
        * json file should contain array of dicts: [{'key': ..., 'value': ...}]

    Examples:

        >>> class MyTestCase(SparklySession):
        ...     fixtures = [
        ...         KafkaFixture(
        ...             'kafka.host', 'topic',
        ...             key_serializer=..., value_serializer=...,
        ...             data='/path/to/data.json',
        ...         )
        ...     ]

    """
    def __init__(self, host, port=9092, topic=None,
                 key_serializer=None, value_serializer=None,
                 data=None):
        """Constructor.

        Args:
            host (str): Kafka host.
            port (int): Kafka port.
            topic (str): Kafka topic.
            key_serializer (function): Converts python data structure to bytes,
                applied to message key.
            value_serializer (function): Converts python data structure to bytes,
                applied to message value.
            data (str): Path to json file with data.
        """
        if not KAFKA_FIXTURES_SUPPORT:
            raise NotImplementedError('kafka-python package isn\'t available. '
                                      'Use pip install sparkly[test] to fix it.')

        self.host = host
        self.port = port
        self.topic = topic
        self.key_serializer = key_serializer
        self.value_serializer = value_serializer
        self.data = data

    def _publish_data(self, data):
        producer = KafkaProducer(bootstrap_servers='kafka.docker',
                                 key_serializer=self.key_serializer,
                                 value_serializer=self.value_serializer)
        for item in data:
            producer.send(self.topic, key=item['key'], value=item['value'])

        producer.flush()
        producer.close()

    def setup_data(self):
        data = [json.loads(item) for item in self.read_file(self.data).strip().split('\n')]
        self._publish_data(data)

    def teardown_data(self):
        pass


class KafkaWatcher:
    """Context manager that tracks Kafka data published to a topic

    Provides access to the new items that were written to a kafka topic by code running
    within this context.

    NOTE: This is mainly useful in integration test cases and may produce unexpected results in
    production environments, since there are no guarantees about who else may be publishing to
    a kafka topic.

    Usage:
        my_deserializer = lambda item: json.loads(item.decode('utf-8'))
        kafka_watcher = KafkaWatcher(
            my_sparkly_session,
            expected_output_dataframe_schema,
            my_deserializer,
            my_deserializer,
            'my.kafkaserver.net',
            'my_kafka_topic',
        )
        with kafka_watcher:
            # do stuff that publishes messages to 'my_kafka_topic'
        self.assertEqual(kafka_watcher.count, expected_number_of_new_messages)
        self.assertDataFrameEqual(kafka_watcher.df, expected_df)
    """

    def __init__(
        self,
        spark,
        df_schema,
        key_deserializer,
        value_deserializer,
        host,
        topic,
        port=9092,
    ):
        """Initialize context manager

        Parameters `key_deserializer` and `value_deserializer` are callables
        which get bytes as input and should return python structures as output.

        Args:
            spark (SparklySession): currently active SparklySession
            df_schema (pyspark.sql.types.StructType): schema of dataframe to be generated
            key_deserializer (function): function used to deserialize the key
            value_deserializer (function): function used to deserialize the value
            host (basestring): host or ip address of the kafka server to connect to
            topic (basestring): Kafka topic to monitor
            port (int): port number of the Kafka server to connect to
        """
        self.spark = spark
        self.topic = topic
        self.df_schema = df_schema
        self.key_deser, self.val_deser = key_deserializer, value_deserializer
        self.host, self.port = host, port
        self._df = None
        self.count = 0

        kafka_client = SimpleClient(host)
        kafka_client.ensure_topic_exists(topic)

    def __enter__(self):
        self._df = None
        self.count = 0
        self.pre_offsets = kafka_get_topics_offsets(
            topic=self.topic,
            host=self.host,
            port=self.port,
        )

    def __exit__(self, e_type, e_value, e_trace):
        self.post_offsets = kafka_get_topics_offsets(
            topic=self.topic,
            host=self.host,
            port=self.port,
        )
        self.count = sum([
            post[2] - pre[2]
            for pre, post in zip(self.pre_offsets, self.post_offsets)
        ])

    @property
    def df(self):
        if not self.count:
            return None
        if not self._df:
            offset_ranges = [
                [pre[0], pre[2], post[2]]
                for pre, post in zip(self.pre_offsets, self.post_offsets)
            ]
            self._df = self.spark.read_ext.kafka(
                topic=self.topic,
                offset_ranges=offset_ranges,
                schema=self.df_schema,
                key_deserializer=self.key_deser,
                value_deserializer=self.val_deser,
                host=self.host,
                port=self.port,
            )
        return self._df
