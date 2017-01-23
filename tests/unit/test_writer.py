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

from pyspark.sql import DataFrame

import sparkly
from sparkly.writer import SparklyWriter


class TestWriteByUrl(unittest.TestCase):
    def setUp(self):
        self.df = mock.Mock(spec=DataFrame)
        self.df.sql_ctx = mock.Mock(spec=sparkly.SparklySession)
        self.write_ext = SparklyWriter(self.df)

    def test_parquet_s3(self):
        self.write_ext.by_url(
            'parquet:s3://my-bucket/path/to/parquet?partitionBy=x,y,z&mode=append&'
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
        self.write_ext.csv = mock.Mock()

        self.write_ext.by_url('csv:///my-bucket/path/to/csv')

        self.write_ext.csv.assert_called_once_with(
            path='/my-bucket/path/to/csv',
            mode=None,
            parallelism=None,
            options={},
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
            port=None,
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
            port=None,
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
            port=None,
            mode=None,
            parallelism=20,
            options={},
        )
