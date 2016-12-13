import json
import uuid

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from sparkly.exceptions import InvalidArgumentError
from sparkly.testing import (
    SparklyGlobalContextTest,
    CassandraFixture,
    MysqlFixture,
    ElasticFixture,
)
from sparkly.utils import absolute_path, kafka_get_topics_offsets
from tests.integration.base import _TestContext


class SparklyReaderCassandraTest(SparklyGlobalContextTest):
    context = _TestContext

    fixtures = [
        CassandraFixture(
            'cassandra.docker',
            absolute_path(__file__, 'resources', 'test_read', 'cassandra_setup.cql'),
            absolute_path(__file__, 'resources', 'test_read', 'cassandra_teardown.cql'),
        )
    ]

    def test_read(self):
        df = self.hc.read_ext.cassandra(
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


class SparklyReaderCSVTest(SparklyGlobalContextTest):
    context = _TestContext

    def test_csv(self):
        df = self.hc.read_ext.csv(
            path=absolute_path(__file__, 'resources', 'test_read', 'test.csv'),
        )

        self.assertDataFrameEqual(df.take(2), [
            {
                'county': 'CLAY COUNTY',
                'statecode': 'FL',
                'point_granularity': 1,
                'point_latitude': 30.102261,
                'fr_site_limit': 498960.0,
                'point_longitude': -81.711777,
                'tiv_2012': 792148.9,
                'fl_site_deductible': 0,
                'fr_site_deductible': 0,
                'construction': 'Masonry',
                'tiv_2011': 498960.0,
                'line': 'Residential',
                'eq_site_deductible': 0,
                'policyID': 119736,
                'eq_site_limit': 498960.0,
                'hu_site_limit': 498960.0,
                'hu_site_deductible': 9979.2,
                'fl_site_limit': 498960.0,
            },
            {
                'county': 'CLAY COUNTY',
                'statecode': 'FL',
                'point_granularity': 3,
                'point_latitude': 30.063936,
                'fr_site_limit': 1322376.3,
                'point_longitude': -81.707664,
                'tiv_2012': 1438163.57,
                'fl_site_deductible': 0,
                'fr_site_deductible': 0,
                'construction': 'Masonry',
                'tiv_2011': 1322376.3,
                'line': 'Residential',
                'eq_site_deductible': 0,
                'policyID': 448094,
                'eq_site_limit': 1322376.3,
                'hu_site_limit': 1322376.3,
                'hu_site_deductible': 0.0,
                'fl_site_limit': 1322376.3,
            },
        ])


class SparklyReaderElasticTest(SparklyGlobalContextTest):
    context = _TestContext

    fixtures = [
        ElasticFixture(
            'elastic.docker',
            'sparkly_test',
            'test',
            None,
            absolute_path(__file__, 'resources', 'test_read', 'elastic_setup.json'),
        )
    ]

    def test_elastic(self):
        df = self.hc.read_ext.elastic(
            host='elastic.docker',
            port=9200,
            es_index='sparkly_test',
            es_type='test',
            query='?q=name:*Smith*',
            options={
                'es.read.field.as.array.include': 'topics',
                'es.read.metadata': 'false',
            },
        )

        self.assertDataFrameEqual(df, [
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
        ])


class SparklyReaderMySQLTest(SparklyGlobalContextTest):
    context = _TestContext

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
        df = self.hc.read_ext.mysql(
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


class TestReaderKafka(SparklyGlobalContextTest):
    context = _TestContext

    KAFKA_TEST_DATA = [
        {'key': {'name': 'john'}, 'value': {'name': 'john', 'surname': 'smith', 'age': 1}},
        {'key': {'name': 'john'}, 'value': {'name': 'john', 'surname': 'smith', 'age': 2}},
        {'key': {'name': 'john'}, 'value': {'name': 'john', 'surname': 'mnemonic', 'age': 3}},
        {'key': {'name': 'john'}, 'value': {'name': 'john', 'surname': 'mnemonic', 'age': 4}},
        {'key': {'name': 'john'}, 'value': {'name': 'john', 'surname': 'smith', 'age': 5}},
        {'key': {'name': 'john'}, 'value': {'name': 'john', 'surname': 'smith', 'age': 6}},
        {'key': {'name': 'john'}, 'value': {'name': 'john', 'surname': 'mnemonic', 'age': 7}},
        {'key': {'name': 'john'}, 'value': {'name': 'john', 'surname': 'mnemonic', 'age': 8}},
        {'key': {'name': 'john'}, 'value': {'name': 'john', 'surname': 'smith', 'age': 9}},
        {'key': {'name': 'john'}, 'value': {'name': 'john', 'surname': 'smith', 'age': 10}},
        {'key': {'name': 'john'}, 'value': {'name': 'john', 'surname': 'mnemonic', 'age': 11}},
        {'key': {'name': 'john'}, 'value': {'name': 'john', 'surname': 'mnemonic', 'age': 12}},
    ]

    KAFKA_TEST_DATA_SCHEMA = StructType([
        StructField('key', StructType([
            StructField('name', StringType(), True)
        ])),
        StructField('value', StructType([
            StructField('name', StringType(), True),
            StructField('surname', StringType(), True),
            StructField('age', IntegerType(), True),
        ]))
    ])

    def setUp(self):
        self.json_decoder = lambda item: json.loads(item.decode('utf-8'))
        self.json_encoder = lambda item: json.dumps(item).encode('utf-8')
        self.topic = 'test.topic.write.kafka.{}'.format(uuid.uuid4().hex[:10])
        rdd = self.hc._sc.parallelize(self.KAFKA_TEST_DATA)
        self.df = self.hc.createDataFrame(rdd, schema=self.KAFKA_TEST_DATA_SCHEMA)
        self.df.write_ext.kafka(
            'kafka.docker',
            self.topic,
            key_serializer=self.json_encoder,
            value_serializer=self.json_encoder,
        )

    def test_read_by_topic(self):
        df = self.hc.read_ext.kafka(
            'kafka.docker',
            topic=self.topic,
            key_deserializer=self.json_decoder,
            value_deserializer=self.json_decoder,
            schema=self.KAFKA_TEST_DATA_SCHEMA,
        )
        self.assertDataFrameEqual(df, self.KAFKA_TEST_DATA)

    def test_read_by_offsets(self):
        offsets = kafka_get_topics_offsets('kafka.docker', self.topic)
        df = self.hc.read_ext.kafka(
            'kafka.docker',
            topic=self.topic,
            offset_ranges=offsets,
            key_deserializer=self.json_decoder,
            value_deserializer=self.json_decoder,
            schema=self.KAFKA_TEST_DATA_SCHEMA,
        )

        self.assertDataFrameEqual(df, self.KAFKA_TEST_DATA)

        self.df.write_ext.kafka(
            'kafka.docker',
            self.topic,
            key_serializer=self.json_encoder,
            value_serializer=self.json_encoder,
        )

    def test_argument_errors(self):
        with self.assertRaises(InvalidArgumentError):
            self.hc.read_ext.kafka(
                'kafka.docker',
                topic=self.topic,
                key_deserializer=self.json_decoder,
                value_deserializer=self.json_decoder,
            )

        with self.assertRaises(InvalidArgumentError):
            self.hc.read_ext.kafka(
                'kafka.docker',
                topic=self.topic,
                schema=self.KAFKA_TEST_DATA_SCHEMA,
            )
