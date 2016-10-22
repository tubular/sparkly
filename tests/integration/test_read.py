import json
import uuid

from sparkle.utils import absolute_path
from sparkle.read import elastic, csv, cassandra, mysql, kafka
from sparkle.test import (
    SparkleGlobalContextTest,
    BaseCassandraTest,
    BaseElasticTest,
    BaseMysqlTest,
)
from tests.integration.base import _TestContext


class TestReadCsv(SparkleGlobalContextTest):

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


class TestReadCassandra(BaseCassandraTest, SparkleGlobalContextTest):

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


class TestReadElastic(BaseElasticTest, SparkleGlobalContextTest):

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


class TestReadMysql(BaseMysqlTest, SparkleGlobalContextTest):

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
