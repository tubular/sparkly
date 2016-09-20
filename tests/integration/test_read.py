import json
import uuid

from kafka import KafkaProducer

from sparkle.utils import absolute_path
from sparkle.read import elastic, csv, cassandra, mysql, kafka
from sparkle.test import SparkleTest, BaseCassandraTest, BaseElasticTest, BaseMysqlTest
from tests.integration.base import _TestContext


class TestReadCsv(SparkleTest):

    context = _TestContext

    def test_csv(self):
        csv_filepath = absolute_path(__file__, 'resources', 'test_read', 'test.csv')

        reader = csv(self.hc, csv_filepath)

        res = reader.take(3)
        self.assertEqual([r.asDict() for r in res],
                         [{'county': 'CLAY COUNTY', 'statecode': 'FL', 'point_granularity': 1,
                           'point_latitude': 30.102261, 'fr_site_limit': 498960.0,
                           'point_longitude': -81.711777, 'tiv_2012': 792148.9,
                           'fl_site_deductible': 0, 'fr_site_deductible': 0,
                           'construction': 'Masonry', 'tiv_2011': 498960.0,
                           'line': 'Residential', 'eq_site_deductible': 0,
                           'policyID': 119736, 'eq_site_limit': 498960.0,
                           'hu_site_limit': 498960.0, 'hu_site_deductible': 9979.2,
                           'fl_site_limit': 498960.0},
                          {'county': 'CLAY COUNTY', 'statecode': 'FL',
                           'point_granularity': 3, 'point_latitude': 30.063936,
                           'fr_site_limit': 1322376.3, 'point_longitude': -81.707664,
                           'tiv_2012': 1438163.57, 'fl_site_deductible': 0,
                           'fr_site_deductible': 0, 'construction': 'Masonry',
                           'tiv_2011': 1322376.3, 'line': 'Residential',
                           'eq_site_deductible': 0, 'policyID': 448094,
                           'eq_site_limit': 1322376.3, 'hu_site_limit': 1322376.3,
                           'hu_site_deductible': 0.0, 'fl_site_limit': 1322376.3},
                          {'county': 'CLAY COUNTY', 'statecode': 'FL', 'point_granularity': 1,
                           'point_latitude': 30.089579, 'fr_site_limit': 190724.4,
                           'point_longitude': -81.700455, 'tiv_2012': 192476.78,
                           'fl_site_deductible': 0, 'fr_site_deductible': 0,
                           'construction': 'Wood', 'tiv_2011': 190724.4,
                           'line': 'Residential', 'eq_site_deductible': 0,
                           'policyID': 206893, 'eq_site_limit': 190724.4,
                           'hu_site_limit': 190724.4, 'hu_site_deductible': 0.0,
                           'fl_site_limit': 190724.4}])


class TestReadCassandra(BaseCassandraTest):

    context = _TestContext

    cql_setup_files = [
        absolute_path(__file__, 'resources', 'test_read', 'cassandra_setup.cql'),
    ]

    cql_teardown_files = [
        absolute_path(__file__, 'resources', 'test_read', 'cassandra_teardown.cql'),
    ]

    def test_read_cassandra(self):
        reader = cassandra(self.hc, self.c_host, 'sparkle_test', 'test', consistency='ONE')
        res = reader.take(3)
        self.assertEqual(
            [r.asDict() for r in res],
            [{'countries': {'DZ': 1, 'EG': 206, 'BE': 1, 'CA': 1, 'AE': 13, 'BH': 3},
              'uid': '6', 'created': '1234567894'},
             {'countries': {'DZ': 1, 'EG': 206, 'BE': 1, 'CA': 1, 'AE': 13, 'BH': 3},
              'uid': '7', 'created': '1234567893'},
             {'countries': {'DZ': 1, 'EG': 206, 'BE': 1, 'CA': 1, 'AE': 13, 'BH': 3},
              'uid': '9', 'created': '1234567891'}])


class TestReadElastic(BaseElasticTest):

    context = _TestContext

    elastic_setup_files = [
        absolute_path(__file__, 'resources', 'test_read', 'elastic_setup.json'),
    ]
    elastic_teardown_indexes = ['sparkle_test']

    def test_read_elastic(self):
        reader = elastic(self.hc, self.elastic_host, 'sparkle_test', 'test',
                         query='?q=name:*Smith*',
                         options={'es.read.field.as.array.include': 'topics'})

        res = reader.take(3)

        self.assertEqual(len(res), 2)

        res = [r.asDict() for r in res]
        for r in res:
            r['demo'] = r['demo'].asDict()

        self.assertEqual(
            res,
            [{'_metadata': {'_index': 'sparkle_test',
                            '_id': '2', '_type': 'test', '_score': '0.0'},
              'age': 31,
              'topics': [1, 4, 5],
              'demo': {'age_10': 50, 'age_30': 110},
              'name': 'Smith3'},
             {'_metadata': {'_index': 'sparkle_test',
                            '_id': '3', '_type': 'test', '_score': '0.0'},
              'age': 12,
              'topics': [4, 5],
              'demo': {'age_10': 1, 'age_30': 20},
              'name': 'Smith4'}]
        )


class TestReadMysql(BaseMysqlTest):

    context = _TestContext

    sql_setup_files = [
        absolute_path(__file__, 'resources', 'test_read', 'mysql_setup.sql'),
    ]

    sql_teardown_files = [
        absolute_path(__file__, 'resources', 'test_read', 'mysql_teardown.sql'),
    ]

    def test_read_mysql(self):
        reader = mysql(self.hc, self.mysql_host, 'sparkle_test', 'test',
                       options={'user': 'root', 'password': ''})

        res = reader.take(3)
        res = [r.asDict() for r in res]
        self.assertEqual(
            res,
            [{'id': 1, 'name': 'john', 'surname': 'sk', 'age': 111},
             {'id': 2, 'name': 'john', 'surname': 'po', 'age': 222},
             {'id': 3, 'name': 'john', 'surname': 'ku', 'age': 333}])


class TestReadKafka(SparkleTest):

    context = _TestContext

    def setUp(self):
        super(TestReadKafka, self).setUp()
        self.kafka_host = 'kafka.docker'
        self._setup_data()

    def _setup_data(self):
        producer = KafkaProducer(bootstrap_servers='{}:9092'.format(self.kafka_host))
        self.test_topic = 'test_topic_{}'.format(uuid.uuid4().hex[:10])
        self.test_topic_2 = 'test_topic_2_{}'.format(uuid.uuid4().hex[:10])
        for i in range(5):
            producer.send(self.test_topic,
                          json.dumps({'message': 'body',
                                      'count': i,
                                      'float': 0.09 * i}).encode('utf-8'),
                          partition=0)
            producer.send(self.test_topic_2,
                          json.dumps({'name': 'johnny',
                                      'count': i
                                      }).encode('utf-8'),
                          partition=0)
        producer.flush()

    def test_simple_read(self):
        df = kafka(self.hc,
                   brokers=['{}:9092'.format(self.kafka_host)],
                   offset_ranges=[(self.test_topic, 0, 0, 2)],
                   )
        res = df.collect()
        self.assertEqual([
            (None, {'count': 0, 'message': 'body', 'float': 0.0}),
            (None, {'count': 1, 'message': 'body', 'float': 0.09})],
            res)

    def test_read_multiple_topics(self):
        df = kafka(self.hc,
                   brokers=['{}:9092'.format(self.kafka_host)],
                   offset_ranges=[(self.test_topic, 0, 0, 2),
                                  (self.test_topic_2, 0, 0, 2)],
                   )
        res = df.collect()
        self.assertEqual([
            (None, {'count': 0, 'message': 'body', 'float': 0.0}),
            (None, {'count': 1, 'message': 'body', 'float': 0.09}),
            (None, {'count': 0, 'name': 'johnny'}),
            (None, {'count': 1, 'name': 'johnny'}),
        ], res)
