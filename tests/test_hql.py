from unittest import TestCase
from pyspark.sql.types import StructType, StringType, StructField
from sparkle.hql import _type_to_hql, get_create_table_statement


class TestHql(TestCase):

    def test_type_to_hql(self):
        res = _type_to_hql({
            'fields': [{
                'metadata': {},
                'name': 'uid',
                'nullable': True,
                'type': 'string'
            }, {
                'metadata': {},
                'name': 'countries',
                'nullable': True,
                'type': {
                    'keyType': 'string',
                    'type': 'map',
                    'valueContainsNull': True,
                    'valueType': 'long'
                }
            }, {
                'metadata': {},
                'name': 'created_at',
                'nullable': True,
                'type': 'timestamp'
            }, {
                'metadata': {},
                'name': 'date',
                'nullable': True,
                'type': 'string'
            }, {
                'metadata': {},
                'name': 'by_countries',
                'nullable': True,
                'type': {
                    'containsNull': True,
                    'elementType': {
                        'fields': [{
                            'metadata': {},
                            'name': 'country',
                            'nullable': True,
                            'type': 'string'
                        }, {
                            'metadata': {},
                            'name': 'views',
                            'nullable': True,
                            'type': 'long'
                        }],
                        'type': 'struct'
                    },
                    'type': 'array'
                }
            }],
            'type': 'struct'
        })

        self.assertEqual(
            res,
            'struct<'
            '`uid`:string,'
            '`countries`:map<string,bigint>,'
            '`created_at`:timestamp,'
            '`date`:string,'
            '`by_countries`:array<struct<`country`:string,`views`:bigint>>>'
        )

    def test_get_create_table_sql(self):
        result = get_create_table_statement(
            table_name='facebook_storyteller',
            schema={
                'fields': [{
                    'metadata': {},
                    'name': 'uid',
                    'nullable': True,
                    'type': 'string'
                }, {
                    'metadata': {},
                    'name': 'countries',
                    'nullable': True,
                    'type': {
                        'keyType': 'string',
                        'type': 'map',
                        'valueContainsNull': True,
                        'valueType': 'long'
                    }
                }, {
                    'metadata': {},
                    'name': 'created_at',
                    'nullable': True,
                    'type': 'long'
                }, {
                    'metadata': {},
                    'name': 'date',
                    'nullable': True,
                    'type': 'string'
                }, {
                    'metadata': {},
                    'name': 'by_countries',
                    'nullable': True,
                    'type': {
                        'containsNull': True,
                        'elementType': {
                            'fields': [{
                                'metadata': {},
                                'name': 'country',
                                'nullable': True,
                                'type': 'string'
                            }, {
                                'metadata': {},
                                'name': 'views',
                                'nullable': True,
                                'type': 'long'
                            }],
                            'type': 'struct'
                        },
                        'type': 'array'
                    }
                }],
                'type': 'struct'
            }, location='s3://fb-storyteller-bucket')

        self.assertEqual(
            result,
            "CREATE EXTERNAL TABLE `facebook_storyteller` "
            "(`uid` string, `countries` map<string,bigint>, `created_at` bigint, `date` string, "
            "`by_countries` array<struct<`country`:string,`views`:bigint>>) "
            "STORED AS PARQUET LOCATION 's3://fb-storyteller-bucket'"
        )

    def test_get_create_table_sql_partition_by(self):
        result = get_create_table_statement(
            table_name='test',
            schema={
                'fields': [{
                    'metadata': {},
                    'name': 'uid',
                    'nullable': True,
                    'type': 'string'
                }, {
                    'metadata': {},
                    'name': 'date',
                    'nullable': True,
                    'type': 'string'
                }, {
                    'metadata': {},
                    'name': 'shmate',
                    'nullable': True,
                    'type': 'string'
                }],
                'type': 'struct'
            },
            location='s3://fb-storyteller-bucket',
            partition_by=['uid', 'date']
        )

        self.assertEqual(
            result,
            "CREATE EXTERNAL TABLE `test` "
            "(`shmate` string) "
            "PARTITIONED BY (`uid` string, `date` string) "
            "STORED AS PARQUET LOCATION 's3://fb-storyteller-bucket'"
        )

    def test__type_to_sql_inner_struct(self):
        res = _type_to_hql({
            'type': {
                'type': 'struct',
                'fields': [
                    {'type': 'long', 'metadata': {}, 'name': 'age_10', 'nullable': True},
                    {'type': 'long', 'metadata': {}, 'name': 'age_30', 'nullable': True}
                ]
            }, 'metadata': {}, 'name': 'demo', 'nullable': True
        })

        self.assertEqual(res, 'struct<`age_10`:bigint,`age_30`:bigint>')

    def test_get_create_table_sql_schema(self):
        res = get_create_table_statement('test',
                                         StructType([StructField("f1", StringType(), True)]),
                                         location='s3://test/')
        self.assertEqual(res,
                         "CREATE EXTERNAL TABLE `test` (`f1` string) "
                         "STORED AS PARQUET LOCATION 's3://test/'")
