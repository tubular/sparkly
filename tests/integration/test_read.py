import unittest

import os
from sparkle import SparkleContext, absolute_path
from sparkle.read import elastic, csv, cassandra, mysql
from sparkle.test import SparkleTest


class _TestContext(SparkleContext):
    packages = ['datastax:spark-cassandra-connector:1.5.0-M3-s_2.10',
                'org.elasticsearch:elasticsearch-spark_2.10:2.3.0',
                'com.databricks:spark-csv_2.10:1.4.0',
                ]


class TestReadCsv(SparkleTest):

    context = _TestContext

    def test_csv(self):
        csv_filepath = absolute_path(__file__, 'resources', 'test.csv')

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


class TestReadCassandra(SparkleTest):

    context = _TestContext

    def setUp(self):
        super(TestReadCassandra, self).setUp()
        self.c_host = 'cassandra'
        self._setup_data()

    def tearDown(self):
        super(TestReadCassandra, self).tearDown()
        self._clear_data()

    def _setup_data(self):
        os.system('source venv2/bin/activate && cqlsh -f {} {}'.format(
            absolute_path(__file__, 'resources', 'cassandra_setup.cql'),
            self.c_host))

    def _clear_data(self):
        os.system('source venv2/bin/activate && cqlsh -f {} {}'.format(
            absolute_path(__file__,
                          'resources',
                          'cassandra_teardown.cql'),
            self.c_host))

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


class TestReadElastic(SparkleTest):

    context = _TestContext

    def setUp(self):
        super(TestReadElastic, self).setUp()
        self.es_host = 'elastic'
        self._setup_data()

    def tearDown(self):
        super(TestReadElastic, self).tearDown()
        self._clear_data()

    def _setup_data(self):
        data_file = absolute_path(__file__, 'resources', 'elastic_setup.json')

        os.system('curl -XPOST \'http://{}:9200/_bulk\' --data-binary @{}'.format(
            self.es_host,
            data_file))

    def _clear_data(self):
        os.system('curl -XDELETE \'http://{}:9200/sparkle_test/test')

    def test_read_elastic(self):
        reader = elastic(self.hc, self.es_host, 'sparkle_test', 'test',
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


class TestReadMysql(SparkleTest):

    context = _TestContext

    def setUp(self):
        super(TestReadMysql, self).setUp()
        self.mysql_host = 'mysql'
        self._setup_data()

    def tearDown(self):
        super(TestReadMysql, self).tearDown()
        self._clear_data()

    def _setup_data(self):
        data_file = absolute_path(__file__, 'resources', 'mysql_setup.sql')
        os.system('mysql -hmysql -uroot < {}'.format(data_file))

    def _clear_data(self):
        data_file = absolute_path(__file__, 'resources', 'mysql_teardown.sql')
        os.system('mysql -hmysql -uroot < {}'.format(data_file))

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


if __name__ == '__main__':
    unittest.main()
