from shutil import rmtree
from tempfile import mkdtemp

from sparkle.utils import absolute_path
from sparkle.test import (
    SparkleGlobalContextTest,
    CassandraFixture,
    ElasticFixture,
    MysqlFixture,
)
from tests.integration.base import _TestContext

TEST_COLUMNS = [
    'video_uid', 'account_uid', 'title', 'description', 'views', 'published'
]

TEST_DATA = [
    ('v1', 'a1', 'Nice Video', 'Nice Nice Nice Video', 10000, '20120305T142153Z'),
    ('v2', 'a2', 'Nice Video', 'Nice Nice Video', 1000, '20120305T142153Z'),
    ('v3', 'a3', 'Nice Video', 'Nice Nice Video', 100, '20120305T142153Z'),
    ('v4', 'a1', 'Nice Video', 'Nice Nice Nice', 10000, '20120305T142153Z'),
]


class TestWriteByURL(SparkleGlobalContextTest):
    context = _TestContext

    def setUp(self):
        self.temp_dir = mkdtemp()

    def tearDown(self):
        rmtree(self.temp_dir)

    def test_write_csv(self):
        dst_path = '{}/test_csv'.format(self.temp_dir)
        df = self.hc.createDataFrame(TEST_DATA, TEST_COLUMNS)

        df.write_ext.by_url('csv://{}?mode=overwrite&header=true'.format(dst_path))

        written_df = self.hc.read_ext.by_url('csv://{}'.format(dst_path))
        self.assertDataframeEqual(written_df, TEST_DATA, TEST_COLUMNS)

    def test_write_parquet(self):
        dst_path = '{}/test_parquet'.format(self.temp_dir)
        df = self.hc.createDataFrame(TEST_DATA, TEST_COLUMNS)

        df.write_ext.by_url('parquet://{}?mode=overwrite'.format(dst_path))

        written_df = self.hc.read_ext.by_url('parquet://{}'.format(dst_path))
        self.assertDataframeEqual(written_df, TEST_DATA, TEST_COLUMNS)


class TestWriteCassandra(SparkleGlobalContextTest):
    context = _TestContext

    fixtures = [
        CassandraFixture(
            'cassandra.docker',
            absolute_path(__file__, 'resources', 'test_write', 'cassandra_setup.cql'),
            absolute_path(__file__, 'resources', 'test_write', 'cassandra_teardown.cql'),
        )
    ]

    def test_write_cassandra(self):
        df = self.hc.createDataFrame(TEST_DATA, TEST_COLUMNS)

        df.write_ext.cassandra(
            'cassandra.docker',
            'sparkle_test',
            'test_writer',
            consistency='ONE',
            mode='overwrite',
        )

        written_df = self.hc.read_ext.by_url(
            'cassandra://cassandra.docker/'
            'sparkle_test/test_writer'
            '?consistency=ONE'
        )
        self.assertDataframeEqual(written_df, TEST_DATA, TEST_COLUMNS)


class TestWriteElastic(SparkleGlobalContextTest):
    context = _TestContext

    fixtures = [
        ElasticFixture(
            'elastic.docker',
            'sparkle_test',
            'test',
            None,
            absolute_path(__file__, 'resources', 'test_write', 'elastic_setup.json')
        )
    ]

    def test_write_elastic(self):
        df = self.hc.createDataFrame(TEST_DATA, TEST_COLUMNS)

        df.write_ext.elastic(
            'elastic.docker',
            'sparkle_test',
            'test_writer',
            mode='overwrite',
            options={
                'es.mapping.id': 'video_uid',
            }
        )

        df = self.hc.read_ext.by_url(
            'elastic://elastic.docker/sparkle_test/test_writer'
        )
        self.assertDataframeEqual(df, TEST_DATA, TEST_COLUMNS)


class TestWriteMysql(SparkleGlobalContextTest):

    context = _TestContext

    fixtures = [
        MysqlFixture(
            'mysql.docker',
            'root',
            None,
            absolute_path(__file__, 'resources', 'test_write', 'mysql_setup.sql'),
            absolute_path(__file__, 'resources', 'test_write', 'mysql_teardown.sql'),
        )
    ]

    def test_write_mysql(self):
        df = self.hc.createDataFrame(TEST_DATA, TEST_COLUMNS)

        df.write_ext.mysql(
            'mysql.docker',
            'sparkle_test',
            'test_writer',
            mode='overwrite',
            options={'user': 'root', 'password': ''}
        )

        df = self.hc.read_ext.by_url(
            'mysql://mysql.docker/'
            'sparkle_test/test_writer'
            '?user=root&password='
        )
        self.assertDataframeEqual(df, TEST_DATA, TEST_COLUMNS)
