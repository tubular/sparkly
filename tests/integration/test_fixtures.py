import pytest

from sparkle.test import CassandraFixture, MysqlFixture, ElasticFixture, SparkleTest, SparkleGlobalContextTest
from sparkle.utils import absolute_path
from tests.integration.base import _TestContext


class TestCassandraFixtures(SparkleGlobalContextTest):

    context = _TestContext

    data = CassandraFixture(
        'cassandra.docker',
        absolute_path(__file__, 'resources', 'test_fixtures', 'cassandra_setup.cql'),
        absolute_path(__file__, 'resources', 'test_fixtures', 'cassandra_teardown.cql'),
    )

    def test_cassandra_fixture(self):
        with self.assertRaises(Exception):
            self.hc.read_ext.by_url('cassandra://cassandra.docker/sparkle_test/test')

        self.data.setup_data()

        df = self.hc.read_ext.by_url('cassandra://cassandra.docker/sparkle_test/test')
        self.assertDataframeEqual(
            df,
            [('1', '1234567899', {'AE': 13, 'BE': 1, 'BH': 3, 'CA': 1, 'DZ': 1, 'EG': 206})],
            ['uid', 'created', 'countries'],
        )

        self.data.teardown_data()

        with self.assertRaises(Exception):
            self.hc.read_ext.by_url('cassandra://cassandra.docker/sparkle_test/test')


class TestMysqlFixtures(SparkleGlobalContextTest):

    context = _TestContext

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
        df = self.hc.read_ext.by_url('mysql://mysql.docker/sparkle_test/test?user=root&password=')
        self.assertDataframeEqual(
            df,
            [(1, 'john', 'sk', 111)],
            ['id', 'name', 'surname', 'age'],
        )


class TestElasticFixture(SparkleGlobalContextTest):

    context = _TestContext

    class_fixtures = [
        ElasticFixture(
            'elastic.docker',
            'sparkle_test_fixture',
            'test',
            absolute_path(__file__, 'resources', 'test_fixtures', 'mapping.json'),
            absolute_path(__file__, 'resources', 'test_fixtures', 'data.json'),
        )
    ]

    def test_elastic_fixture(self):
        df = self.hc.read_ext.by_url('elastic://elastic.docker/sparkle_test_fixture/test')
        self.assertDataframeEqual(
            df,
            [('John', 56)],
            ['name', 'age'],
        )
