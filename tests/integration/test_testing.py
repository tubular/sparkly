import pytest

from sparkle.testing import (
    CassandraFixture,
    ElasticFixture,
    MysqlFixture,
    SparkleGlobalContextTest,
)
from sparkle.utils import absolute_path
from tests.integration.base import _TestContext


class TestCassandraFixtures(SparkleGlobalContextTest):
    context = _TestContext

    def test_cassandra_fixture(self):
        data_in_cassandra = CassandraFixture(
            'cassandra.docker',
            absolute_path(__file__, 'resources', 'test_fixtures', 'cassandra_setup.cql'),
            absolute_path(__file__, 'resources', 'test_fixtures', 'cassandra_teardown.cql'),
        )

        with data_in_cassandra:
            df = self.hc.read_ext.by_url('cassandra://cassandra.docker/sparkle_test/test')
            self.assertDataFrameEqual(df, [
                {
                    'uid': '1',
                    'countries': {'AE': 13, 'BE': 1, 'BH': 3, 'CA': 1, 'DZ': 1, 'EG': 206},
                },
            ], fields=['uid', 'countries'])


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
        self.assertDataFrameEqual(df, [
            {'id': 1, 'name': 'john', 'surname': 'sk', 'age': 111},
        ])


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
        df = self.hc.read_ext.by_url('elastic://elastic.docker/sparkle_test_fixture/test?'
                                     'es.read.metadata=false')
        self.assertDataFrameEqual(df, [
            {'name': 'John', 'age': 56},
        ])
