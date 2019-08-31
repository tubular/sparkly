#
# Copyright 2017 Tubular Labs, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import unittest
try:
    from unittest import mock
except ImportError:
    import mock

import pyspark.sql

import sparkly
from sparkly.reader import SparklyReader
from sparkly.utils import parse_schema


class TestSparklyReaderByUrl(unittest.TestCase):
    def setUp(self):
        self.spark = mock.Mock(spec=sparkly.SparklySession)
        self.read_ext = SparklyReader(self.spark)
        self.fake_df = mock.Mock(spec=pyspark.sql.DataFrame)

    def test_table(self):
        self.spark.table.return_value = self.fake_df

        df = self.read_ext.by_url('table://some_hive_table')

        self.assertEqual(df, self.fake_df)
        self.spark.table.assert_called_with('some_hive_table')

    def test_parquet(self):
        self.spark.read.load.return_value = self.fake_df

        df = self.read_ext.by_url('parquet:s3://my-bucket/path/to/parquet')

        self.assertEqual(df, self.fake_df)
        self.spark.read.load.assert_called_with(
            path='s3://my-bucket/path/to/parquet',
            format='parquet',
        )

    def test_csv(self):
        self.spark.read.csv.return_value = self.fake_df

        df = self.read_ext.by_url('csv:s3://my-bucket/path/to/csv?header=true')

        self.assertEqual(df, self.fake_df)
        self.spark.read.csv.assert_called_with(
            path='s3://my-bucket/path/to/csv',
            header='true',
        )

    def test_csv_on_local_file_system(self):
        self.spark.read.csv.return_value = self.fake_df

        schema = 'struct<a:string,b:bigint,c:array<int>>'
        df = self.read_ext.by_url('csv:///path/on/file/system?header=false&schema={}'
                                  .format(schema))

        self.assertEqual(df, self.fake_df)
        self.spark.read.csv.assert_called_with(
            path='/path/on/file/system',
            schema=parse_schema(schema),
            header='false',
        )

    def test_elastic_on_or_before_6(self):
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

    def test_elastic_on_and_after_7(self):
        self.read_ext.elastic = mock.Mock(return_value=self.fake_df)

        df = self.read_ext.by_url('elastic://es_host/test_index?'
                                  'q=name:*Johnny*&fields=name,surname&'
                                  'es.input.json=true&parallelism=4')

        self.assertEqual(df, self.fake_df)
        self.read_ext.elastic.assert_called_with(
            host='es_host',
            es_index='test_index',
            es_type=None,
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

    def test_cassandra_custom_port(self):
        self.read_ext.cassandra = mock.Mock(return_value=self.fake_df)

        df = self.read_ext.by_url('cassandra://localhost:19042/test_cf/test_table?'
                                  'consistency=ONE&parallelism=8&query.retry.count=2')

        self.assertEqual(df, self.fake_df)
        self.read_ext.cassandra.assert_called_with(
            host='localhost',
            port=19042,
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

    def test_mysql_custom_port(self):
        self.read_ext.mysql = mock.Mock(return_value=self.fake_df)

        df = self.read_ext.by_url('mysql://localhost:33306/test_database/test_table?'
                                  'user=root&password=pass')

        self.assertEqual(df, self.fake_df)
        self.read_ext.mysql.assert_called_with(
            host='localhost',
            database='test_database',
            table='test_table',
            port=33306,
            parallelism=None,
            options={'user': 'root', 'password': 'pass'},
        )

    def test_unknown_format(self):
        self.assertRaises(NotImplementedError, self.read_ext.by_url, 'fake://host')
