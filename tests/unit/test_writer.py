import unittest
try:
    from unittest import mock
except ImportError:
    import mock

import pytest
from pyspark.sql import DataFrame

import sparkle
from sparkle.writer import SparkleWriter


@pytest.mark.branch_1_0
class TestWriteByUrl(unittest.TestCase):
    def setUp(self):
        self.df = mock.Mock(spec=DataFrame)
        self.df.sql_ctx = mock.Mock(spec=sparkle.SparkleContext)
        self.write_ext = SparkleWriter(self.df)

    def test_parquet_s3(self):
        self.write_ext.by_url(
            'parquet:s3://my-bucket/path/to/parquet?partition_by=x,y,z&mode=append&'
            'additional=1&parallelism=20',
        )

        self.df.coalesce.assert_called_once_with(20)
        self.df.coalesce.return_value.write.save.assert_called_once_with(
            path='s3://my-bucket/path/to/parquet',
            format='parquet',
            mode='append',
            partitionBy=['x', 'y', 'z'],
            additional='1',
        )

    def test_csv_local(self):
        self.write_ext.by_url('csv:///my-bucket/path/to/csv')

        self.df.write.save.assert_called_once_with(
            path='/my-bucket/path/to/csv',
            format='csv',
            mode=None,
            partitionBy=None,
        )

    def test_cassandra(self):
        self.write_ext.cassandra = mock.Mock()

        self.write_ext.by_url(
            'cassandra://host/ks/cf?consistency=ONE&mode=overwrite&parallelism=10',
        )

        self.write_ext.cassandra.assert_called_once_with(
            host='host',
            keyspace='ks',
            table='cf',
            mode='overwrite',
            consistency='ONE',
            parallelism=10,
            options={},
        )

    def test_elastic(self):
        self.write_ext.elastic = mock.Mock()

        self.write_ext.by_url('elastic://host/index/type?parallelism=15')

        self.write_ext.elastic.assert_called_once_with(
            host='host',
            es_index='index',
            es_type='type',
            mode=None,
            parallelism=15,
            options={},
        )

    def test_mysql(self):
        self.write_ext.mysql = mock.Mock()

        self.write_ext.by_url('mysql://host/db/table?parallelism=20')

        self.write_ext.mysql.assert_called_with(
            host='host',
            database='db',
            table='table',
            mode=None,
            parallelism=20,
            options={},
        )
