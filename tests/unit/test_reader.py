import unittest
try:
    from unittest import mock
except ImportError:
    import mock

import pytest
import pyspark.sql

import sparkle
from sparkle.reader import SparkleReader
from sparkle.schema_parser import generate_structure_type, parse_schema


@pytest.mark.branch_1_0
class TestSparkleReaderByUrl(unittest.TestCase):
    def setUp(self):
        self.hc = mock.Mock(spec=sparkle.SparkleContext)
        self.read_ext = SparkleReader(self.hc)
        self.fake_df = mock.Mock(spec=pyspark.sql.DataFrame)

    def test_hive_table(self):
        self.hc.table.return_value = self.fake_df

        df = self.read_ext.by_url('table://some_hive_table')

        self.assertEqual(df, self.fake_df)
        self.hc.table.assert_called_with('some_hive_table')

    def test_parquet(self):
        self.hc.read.format.return_value.options.return_value.load.return_value = self.fake_df

        df = self.read_ext.by_url('parquet:s3://my-bucket/path/to/parquet')

        self.assertEqual(df, self.fake_df)
        self.hc.read.format.return_value.options.return_value.load.assert_called_with(
            's3://my-bucket/path/to/parquet'
        )

    def test_csv(self):
        self.read_ext.csv = mock.Mock(return_value=self.fake_df)

        df = self.read_ext.by_url('csv:s3://my-bucket/path/to/csv?header=false')

        self.assertEqual(df, self.fake_df)
        self.read_ext.csv.assert_called_with('s3://my-bucket/path/to/csv', header=False)

    def test_csv_on_local_file_system(self):
        self.read_ext.csv = mock.Mock(return_value=self.fake_df)
        self.hc.read.format.return_value.options.return_value.load.return_value = self.fake_df

        schema = 'name:string|age:long|l:list[long]|s:struct[name:string,age:long]'
        df = self.read_ext.by_url('csv:///path/on/file/system?header=false&custom_schema={}'
                                  .format(schema))

        self.assertEqual(df, self.fake_df)
        self.read_ext.csv.assert_called_with(
            '/path/on/file/system',
            header=False,
            custom_schema=generate_structure_type(parse_schema(schema)),
        )

    def test_elastic(self):
        self.read_ext.elastic = mock.Mock(return_value=self.fake_df)

        df = self.read_ext.by_url('elastic://es_host/test_index/test_type?'
                                  'q=name:*Johnny*&fields=name,surname&'
                                  'es.input.json=true&parallelism=4')

        self.assertEqual(df, self.fake_df)
        self.read_ext.elastic.assert_called_with(
            'es_host', 'test_index', 'test_type',
            query='?q=name:*Johnny*',
            fields=['name', 'surname'],
            parallelism=4,
            options={'es.input.json': 'true'},
        )

    def test_cassandra(self):
        self.read_ext.cassandra = mock.Mock(return_value=self.fake_df)

        df = self.read_ext.by_url('cassandra://localhost/test_cf/test_table?'
                                  'consistency=ONE&parallelism=8&query.retry.count=2')

        self.assertEqual(df, self.fake_df)
        self.read_ext.cassandra.assert_called_with(
            'localhost', 'test_cf', 'test_table',
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
            'localhost', 'test_database', 'test_table',
            options={'user': 'root', 'password': 'pass'},
        )
