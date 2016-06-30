from unittest import TestCase
import mock
from sparkle.read import by_url


class TestTestByUrl(TestCase):

    @mock.patch('sparkle.read.csv')
    def test_csv(self, csv):
        hc = mock.Mock()
        by_url(hc, 'csv://localhost/test/test.csv')
        csv.assert_called_with(hc, '/test/test.csv', options={})

    @mock.patch('sparkle.read.cassandra')
    def test_cassandra(self, cassandra):
        hc = mock.Mock()
        by_url(hc, 'cassandra://localhost/test_cf/test_table?consistency=ONE&name=john')
        cassandra.assert_called_with(hc, 'localhost', 'test_cf', 'test_table',
                                     consistency='ONE',
                                     options={'name': 'john'})

    @mock.patch('sparkle.read.elastic')
    def test_elastic(self, elastic):
        hc = mock.Mock()
        by_url(hc, 'elastic://localhost/test_index/test_type?'
                   'q=name:*Johnny*&fields=name,surname&name=Johnny')
        elastic.assert_called_with(hc, 'localhost', 'test_index', 'test_type',
                                   query='?q=name:*Johnny*',
                                   fields=['name', 'surname'],
                                   parallelism=None,
                                   options={'name': 'Johnny'})

    @mock.patch('sparkle.read.mysql')
    def test_mysql(self, mysql):
        hc = mock.Mock()
        by_url(hc, 'mysql://localhost/test_db/test_table?user=root&password=pass')
        mysql.assert_called_with(hc, 'localhost', 'test_db', 'test_table',
                                 options={'user': 'root', 'password': 'pass'})

    @mock.patch('sparkle.read.cassandra')
    def test_cassandra_parallelism(self, cassandra):
        hc = mock.Mock()
        by_url(hc, 'cassandra://localhost/test_cf/test_table?'
                   'consistency=ONE&name=john&parallelism=4')
        cassandra.assert_called_with(hc, 'localhost', 'test_cf', 'test_table',
                                     consistency='ONE',
                                     parallelism=4,
                                     options={'name': 'john'})

    @mock.patch('sparkle.read.elastic')
    def test_elastic_parallelism(self, elastic):
        hc = mock.Mock()
        by_url(hc, 'elastic://localhost/test_index/test_type?'
                   'q=name:*Johnny*&fields=name,surname&name=Johnny&parallelism=4')
        elastic.assert_called_with(hc, 'localhost', 'test_index', 'test_type',
                                   query='?q=name:*Johnny*',
                                   fields=['name', 'surname'],
                                   parallelism=4,
                                   options={'name': 'Johnny'})

    @mock.patch('sparkle.read.kafka')
    def test_kafka(self, kafka):
        hc = mock.Mock()
        by_url(hc, 'kafka://localhost,other/test_topic,0,0,100/')
        kafka.assert_called_with(hc,
                                 ['localhost', 'other'],
                                 [('test_topic', 0, 0, 100)],
                                 )

        by_url(hc, 'kafka://localhost:1111/test_topic,0,0,100/test_topic,0,1,10')
        kafka.assert_called_with(hc,
                                 ['localhost:1111'],
                                 [('test_topic', 0, 0, 100),
                                  ('test_topic', 0, 1, 10)])
