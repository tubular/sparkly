import os
import uuid
from shutil import rmtree
from tempfile import mkdtemp

from sparkle.utils import absolute_path

from sparkle import read
from sparkle.write import fs, cassandra, elastic, mysql
from sparkle.test import SparkleTest
from tests.integration.base import _TestContext, BaseMysqlTest, BaseElasticTest, BaseCassandraTest

TEST_COLUMNS = [
    'video_uid', 'account_uid', 'title', 'description', 'views', 'published'
]

TEST_DATA = [
    ('v1', 'a1', 'Nice Video', 'Nice Nice Nice Video', 10000, '20120305T142153Z'),
    ('v2', 'a2', 'Nice Video', 'Nice Nice Video', 1000, '20120305T142153Z'),
    ('v3', 'a3', 'Nice Video', 'Nice Nice Video', 100, '20120305T142153Z'),
    ('v4', 'a1', 'Nice Video', 'Nice Nice Nice', 10000, '20120305T142153Z'),
]


class TestWriteFS(SparkleTest):

    context = _TestContext

    def test_write_to_local_csv(self):
        df = self.hc.createDataFrame(TEST_DATA, TEST_COLUMNS)

        temp_dir = mkdtemp()
        dest_path = '{}/test_csv'.format(temp_dir)
        try:
            fs(df,
               dest_path,
               partition_by=['video_uid'],
               format='csv',
               mode='overwrite',
               options={'header': 'true'},
               )

            df = read.by_url(self.hc, 'csv://{}'.format(dest_path))
            self.assertDataframeEqual(
                df,
                TEST_DATA,
                TEST_COLUMNS,
            )
        finally:
            rmtree(temp_dir)

    def test_write_s3_parquet(self):
        df = self.hc.createDataFrame(TEST_DATA, TEST_COLUMNS)
        dest_path = 's3://tubular-tests/sparkle/{}'.format(uuid.uuid4().hex)

        try:
            fs(df,
               path=dest_path,
               partition_by=['video_uid'],
               mode='overwrite',
               )

            df = read.by_url(self.hc, 'parquet:{}'.format(dest_path))
            self.assertDataframeEqual(df, TEST_DATA, TEST_COLUMNS)
        finally:
            os.system('aws s3 rm --recursive {}'.format(dest_path))


class TestWriteCassandra(BaseCassandraTest):

    cql_setup_files = [
        absolute_path(__file__, 'resources', 'test_write', 'cassandra_setup.cql'),
    ]
    cql_teardown_files = [
        absolute_path(__file__, 'resources', 'test_write', 'cassandra_teardown.cql'),
    ]

    def test_write_cassandra(self):
        df = self.hc.createDataFrame(TEST_DATA, TEST_COLUMNS)

        cassandra(df, self.c_host, 'sparkle_test', 'test_writer',
                  consistency='ONE', mode='overwrite')

        df = read.by_url(
            self.hc,
            'cassandra://{}/sparkle_test/test_writer?consistency=ONE'.format(self.c_host)
        )
        self.assertDataframeEqual(df, TEST_DATA, TEST_COLUMNS)


class TestWriteElastic(BaseElasticTest):

    elastic_setup_files = [
        absolute_path(__file__, 'resources', 'test_write', 'elastic_setup.json')
    ]
    elastic_teardown_indexes = ['sparkle_test']

    def test_write_elastic(self):
        df = self.hc.createDataFrame(TEST_DATA, TEST_COLUMNS)

        elastic(
            df,
            self.es_host,
            'sparkle_test',
            'test_writer',
            mode='overwrite',
            options={
                'es.mapping.id': 'video_uid',
            }
        )

        df = read.by_url(self.hc,
                         'elastic://{}/sparkle_test/test_writer'.format(self.es_host))
        self.assertDataframeEqual(df, TEST_DATA, TEST_COLUMNS)


class TestWriteMysql(BaseMysqlTest):

    sql_setup_files = [
        absolute_path(__file__, 'resources', 'test_write', 'mysql_setup.sql'),
    ]

    sql_teardown_files = [
        absolute_path(__file__, 'resources', 'test_write', 'mysql_teardown.sql'),
    ]

    def test_write_mysql(self):
        df = self.hc.createDataFrame(TEST_DATA, TEST_COLUMNS)

        mysql(df, self.mysql_host, 'sparkle_test', 'test_writer',
              mode='overwrite',
              options={'user': 'root', 'password': ''})

        df = read.by_url(
            self.hc,
            'mysql://{}/sparkle_test/test_writer?'
            'user=root&password='.format(self.mysql_host)
        )
        self.assertDataframeEqual(df, TEST_DATA, TEST_COLUMNS)
