from shutil import rmtree
from tempfile import mkdtemp

import pytest

from sparkle.utils import absolute_path
from sparkle.test import (
    SparkleGlobalContextTest,
    BaseCassandraTest,
    BaseElasticTest,
    BaseMysqlTest,
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


@pytest.mark.branch_1_0
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


@pytest.mark.branch_1_0
class TestWriteCassandra(BaseCassandraTest, SparkleGlobalContextTest):
    context = _TestContext

    cql_setup_files = [
        absolute_path(__file__, 'resources', 'test_write', 'cassandra_setup.cql'),
    ]
    cql_teardown_files = [
        absolute_path(__file__, 'resources', 'test_write', 'cassandra_teardown.cql'),
    ]

    def test_write_cassandra(self):
        df = self.hc.createDataFrame(TEST_DATA, TEST_COLUMNS)

        df.write_ext.cassandra(self.c_host, 'sparkle_test', 'test_writer',
                               consistency='ONE', mode='overwrite')

        written_df = self.hc.read_ext.by_url(
            'cassandra://{}/sparkle_test/test_writer?consistency=ONE'.format(self.c_host)
        )
        self.assertDataframeEqual(written_df, TEST_DATA, TEST_COLUMNS)


@pytest.mark.branch_1_0
class TestWriteElastic(BaseElasticTest, SparkleGlobalContextTest):
    context = _TestContext

    elastic_setup_files = [
        absolute_path(__file__, 'resources', 'test_write', 'elastic_setup.json')
    ]
    elastic_teardown_indexes = ['sparkle_test']

    def test_write_elastic(self):
        df = self.hc.createDataFrame(TEST_DATA, TEST_COLUMNS)

        df.write_ext.elastic(
            self.elastic_host,
            'sparkle_test',
            'test_writer',
            mode='overwrite',
            options={
                'es.mapping.id': 'video_uid',
            }
        )

        df = self.hc.read_ext.by_url(
            'elastic://{}/sparkle_test/test_writer'.format(self.elastic_host)
        )
        self.assertDataframeEqual(df, TEST_DATA, TEST_COLUMNS)


@pytest.mark.branch_1_0
class TestWriteMysql(BaseMysqlTest, SparkleGlobalContextTest):

    context = _TestContext

    sql_setup_files = [
        absolute_path(__file__, 'resources', 'test_write', 'mysql_setup.sql'),
    ]

    sql_teardown_files = [
        absolute_path(__file__, 'resources', 'test_write', 'mysql_teardown.sql'),
    ]

    def test_write_mysql(self):
        df = self.hc.createDataFrame(TEST_DATA, TEST_COLUMNS)

        df.write_ext.mysql(self.mysql_host, 'sparkle_test', 'test_writer',
                           mode='overwrite',
                           options={'user': 'root', 'password': ''})

        df = self.hc.read_ext.by_url(
            'mysql://{}/sparkle_test/test_writer?'
            'user=root&password='.format(self.mysql_host)
        )
        self.assertDataframeEqual(df, TEST_DATA, TEST_COLUMNS)
