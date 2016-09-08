import unittest
try:
    from unittest import mock
except ImportError:
    import mock

import pyspark.sql

from sparkle.schema_parser import generate_structure_type, parse_schema
import sparkle
import sparkle.read


class TestReadByUrl(unittest.TestCase):
    def setUp(self):
        self.hc = mock.Mock(spec=sparkle.SparkleContext)
        self.fake_df = mock.Mock(spec=pyspark.sql.DataFrame)

    def test_hive_table(self):
        self.hc.table.return_value = self.fake_df

        df = sparkle.read.by_url(self.hc, 'table://some_hive_table')

        self.assertEqual(df, self.fake_df)
        self.hc.table.assert_called_with('some_hive_table')

    def test_parquet(self):
        self.hc.read.format.return_value.options.return_value.load.return_value = self.fake_df

        df = sparkle.read.by_url(self.hc, 'parquet:s3://my-bucket/path/to/parquet')

        self.assertEqual(df, self.fake_df)
        self.hc.read.format.return_value.options.return_value.load.assert_called_with(
            's3://my-bucket/path/to/parquet'
        )

    @mock.patch('sparkle.read.csv')
    def test_csv(self, csv_mock):
        csv_mock.return_value = self.fake_df

        df = sparkle.read.by_url(self.hc, 'csv:s3://my-bucket/path/to/csv?header=false')

        self.assertEqual(df, self.fake_df)
        csv_mock.assert_called_with(self.hc, 's3://my-bucket/path/to/csv', header=False)

    @mock.patch('sparkle.read.csv')
    def test_csv_on_local_file_system(self, csv_mock):
        self.hc.read.format.return_value.options.return_value.load.return_value = self.fake_df

        csv_mock.return_value = self.fake_df

        schema = 'name:string|age:long|l:list[long]|s:struct[name:string,age:long]'
        df = sparkle.read.by_url(self.hc, 'csv:///path/on/file/system?header=false&custom_schema='
                                          '{}'.format(schema))

        self.assertEqual(df, self.fake_df)
        csv_mock.assert_called_with(
            self.hc,
            '/path/on/file/system',
            header=False,
            custom_schema=generate_structure_type(parse_schema(schema)),
        )

    @mock.patch('sparkle.read.elastic')
    def test_elastic(self, elastic_mock):
        elastic_mock.return_value = self.fake_df

        df = sparkle.read.by_url(self.hc, 'elastic://es_host/test_index/test_type?'
                                          'q=name:*Johnny*&fields=name,surname&'
                                          'es.input.json=true&parallelism=4')

        self.assertEqual(df, self.fake_df)
        elastic_mock.assert_called_with(self.hc, 'es_host', 'test_index', 'test_type',
                                        query='?q=name:*Johnny*',
                                        fields=['name', 'surname'],
                                        parallelism=4,
                                        options={'es.input.json': 'true'})

    @mock.patch('sparkle.read.cassandra')
    def test_cassandra(self, cassandra_mock):
        cassandra_mock.return_value = self.fake_df

        df = sparkle.read.by_url(self.hc, 'cassandra://localhost/test_cf/test_table?'
                                          'consistency=ONE&parallelism=8&query.retry.count=2')

        self.assertEqual(df, self.fake_df)
        cassandra_mock.assert_called_with(self.hc, 'localhost', 'test_cf', 'test_table',
                                          consistency='ONE',
                                          parallelism=8,
                                          options={'query.retry.count': '2'})

    @mock.patch('sparkle.read.mysql')
    def test_mysql(self, mysql_mock):
        mysql_mock.return_value = self.fake_df

        df = sparkle.read.by_url(self.hc, 'mysql://localhost/test_database/test_table?'
                                          'user=root&password=pass')

        self.assertEqual(df, self.fake_df)
        mysql_mock.assert_called_with(self.hc, 'localhost', 'test_database', 'test_table',
                                      options={'user': 'root', 'password': 'pass'})

    @mock.patch('sparkle.read.kafka')
    def test_kafka(self, kafka):
        sparkle.read.by_url(self.hc, 'kafka://localhost,other/test_topic,0,0,100/')

        kafka.assert_called_with(self.hc,
                                 ['localhost', 'other'],
                                 [('test_topic', 0, 0, 100)],
                                 )

        sparkle.read.by_url(self.hc, 'kafka://localhost:1111/test_topic,0,0,100/test_topic,0,1,10')
        kafka.assert_called_with(self.hc,
                                 ['localhost:1111'],
                                 [('test_topic', 0, 0, 100),
                                  ('test_topic', 0, 1, 10)])
