import unittest
try:
    from unittest import mock
except ImportError:
    import mock

import pytest
import pyspark.sql

import sparkly
from sparkly.reader import SparklyReader
from sparkly import schema_parser


class TestSparklyReaderByUrl(unittest.TestCase):
    def setUp(self):
        self.hc = mock.Mock(spec=sparkly.SparklyContext)
        self.read_ext = SparklyReader(self.hc)
        self.fake_df = mock.Mock(spec=pyspark.sql.DataFrame)

    def test_table(self):
        self.hc.table.return_value = self.fake_df

        df = self.read_ext.by_url('table://some_hive_table')

        self.assertEqual(df, self.fake_df)
        self.hc.table.assert_called_with('some_hive_table')

    def test_parquet(self):
        self.hc.read.load.return_value = self.fake_df

        df = self.read_ext.by_url('parquet:s3://my-bucket/path/to/parquet')

        self.assertEqual(df, self.fake_df)
        self.hc.read.load.assert_called_with(
            path='s3://my-bucket/path/to/parquet',
            format='parquet',
        )

    def test_csv(self):
        self.read_ext.csv = mock.Mock(return_value=self.fake_df)

        df = self.read_ext.by_url('csv:s3://my-bucket/path/to/csv?header=true')

        self.assertEqual(df, self.fake_df)
        self.read_ext.csv.assert_called_with(
            path='s3://my-bucket/path/to/csv',
            header=True,
            parallelism=None,
            options={},
        )

    def test_csv_on_local_file_system(self):
        self.read_ext.csv = mock.Mock(return_value=self.fake_df)
        self.hc.read.format.return_value.options.return_value.load.return_value = self.fake_df

        schema = 'name:string|age:long|l:list[long]|s:struct[name:string,age:long]'
        df = self.read_ext.by_url('csv:///path/on/file/system?header=false&custom_schema={}'
                                  .format(schema))

        self.assertEqual(df, self.fake_df)
        self.read_ext.csv.assert_called_with(
            path='/path/on/file/system',
            custom_schema=schema_parser.parse(schema),
            header=False,
            parallelism=None,
            options={},
        )

    def test_elastic(self):
        self.read_ext.elastic = mock.Mock(return_value=self.fake_df)

        df = self.read_ext.by_url('elastic://es_host/test_index/test_type?'
                                  'q=name:*Johnny*&fields=name,surname&'
                                  'es.input.json=true&parallelism=4')

        self.assertEqual(df, self.fake_df)
        self.read_ext.elastic.assert_called_with(
            host='es_host',
            es_index='test_index',
            es_type='test_type',
            query='?q=name:*Johnny*',
            fields=['name', 'surname'],
            port=None,
            parallelism=4,
            options={'es.input.json': 'true'},
        )

    def test_cassandra(self):
        self.read_ext.cassandra = mock.Mock(return_value=self.fake_df)

        df = self.read_ext.by_url('cassandra://localhost/test_cf/test_table?'
                                  'consistency=ONE&parallelism=8&query.retry.count=2')

        self.assertEqual(df, self.fake_df)
        self.read_ext.cassandra.assert_called_with(
            host='localhost',
            port=None,
            keyspace='test_cf',
            table='test_table',
            consistency='ONE',
            parallelism=8,
            options={'query.retry.count': '2'},
        )

    def test_mysql(self):
        self.read_ext.mysql = mock.Mock(return_value=self.fake_df)

        df = self.read_ext.by_url('mysql://localhost/test_database/test_table?'
                                  'user=root&password=pass')

        self.assertEqual(df, self.fake_df)
        self.read_ext.mysql.assert_called_with(
            host='localhost',
            database='test_database',
            table='test_table',
            port=None,
            parallelism=None,
            options={'user': 'root', 'password': 'pass'},
        )

    def test_unknown_format(self):
        self.assertRaises(NotImplementedError, self.read_ext.by_url, 'fake://host')
