from sparkly.testing import (
    CassandraFixture,
    ElasticFixture,
    MysqlFixture,
    SparklyGlobalContextTest,
)
from sparkly.utils import absolute_path
from tests.integration.base import _TestContext


class TestAssertions(SparklyGlobalContextTest):
    context = _TestContext

    def test_assert_dataframe_equal(self):
        df = self.hc.createDataFrame([('Alice', 1),
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

    def test_assert_rdd_equal(self):
        rdd = self.hc._sc.parallelize([1, 2, 3, 4, 5])

        self.assertRDDEqual(rdd, [1, 2, 3, 4, 5])
        self.assertRDDEqual(rdd, [1, 2, 3, 4, 5], ordered=True)
        with self.assertRaises(AssertionError):
            self.assertRDDEqual(rdd, [3, 4, 5, 1, 2], ordered=True)


class TestCassandraFixtures(SparklyGlobalContextTest):
    context = _TestContext

    def test_cassandra_fixture(self):
        data_in_cassandra = CassandraFixture(
            'cassandra.docker',
            absolute_path(__file__, 'resources', 'test_fixtures', 'cassandra_setup.cql'),
            absolute_path(__file__, 'resources', 'test_fixtures', 'cassandra_teardown.cql'),
        )

        with data_in_cassandra:
            df = self.hc.read_ext.by_url('cassandra://cassandra.docker/sparkly_test/test')
            self.assertDataFrameEqual(df, [
                {
                    'uid': '1',
                    'countries': {'AE': 13, 'BE': 1, 'BH': 3, 'CA': 1, 'DZ': 1, 'EG': 206},
                },
            ], fields=['uid', 'countries'])


class TestMysqlFixtures(SparklyGlobalContextTest):

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
        df = self.hc.read_ext.by_url('mysql://mysql.docker/sparkly_test/test?user=root&password=')
        self.assertDataFrameEqual(df, [
            {'id': 1, 'name': 'john', 'surname': 'sk', 'age': 111},
        ])


class TestElasticFixture(SparklyGlobalContextTest):

    context = _TestContext

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
        df = self.hc.read_ext.by_url('elastic://elastic.docker/sparkly_test_fixture/test?'
                                     'es.read.metadata=false')
        self.assertDataFrameEqual(df, [
            {'name': 'John', 'age': 56},
        ])
