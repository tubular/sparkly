import unittest
try:
    from unittest import mock
except ImportError:
    import mock
import sparkle
import sparkle.write


class TestWriteByUrl(unittest.TestCase):

    @mock.patch('sparkle.write.fs')
    def test_parquet_s3(self, fs_mock):
        df = mock.Mock()
        path = 'parquet:s3://my-bucket/path/to/parquet?partition_by=x,y,z&mode=append&additional=1'

        sparkle.write.by_url(df, path)

        fs_mock.assert_called_with(
            df,
            's3://my-bucket/path/to/parquet',
            partition_by=['x', 'y', 'z'],
            output_format='parquet',
            mode='append',
            options={
                'additional': '1',
            }
        )

    @mock.patch('sparkle.write.fs')
    def test_csv_local(self, fs_mock):
        df = mock.Mock()
        path = 'csv:///my-bucket/path/to/csv'

        sparkle.write.by_url(df, path)

        fs_mock.assert_called_with(
            df,
            '/my-bucket/path/to/csv',
            output_format='csv',
            mode=None,
            options={},
            partition_by=None,
        )

    @mock.patch('sparkle.write.cassandra')
    def test_cassandra(self, cassandra_mock):
        df = mock.Mock()
        path = 'cassandra://host/ks/cf?consistency=ONE&mode=overwrite'

        sparkle.write.by_url(df, path)

        cassandra_mock.assert_called_with(
            df,
            'host',
            keyspace='ks',
            table='cf',
            consistency='ONE',
            mode='overwrite',
            options={},
        )

    @mock.patch('sparkle.write.elastic')
    def test_elastic(self, elastic_mock):
        df = mock.Mock()
        path = 'elastic://host/index/type'

        sparkle.write.by_url(df, path)

        elastic_mock.assert_called_with(
            df,
            'host',
            es_index='index',
            es_type='type',
            mode=None,
            options={},
        )

    @mock.patch('sparkle.write.mysql')
    def test_mysql(self, mysql_mock):
        df = mock.Mock()
        path = 'mysql://host/db/table'

        sparkle.write.by_url(df, path)

        mysql_mock.assert_called_with(
            df,
            'host',
            database='db',
            table='table',
            mode=None,
            options={},
        )
