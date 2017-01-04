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

from unittest import TestCase

from pyspark.sql.types import (StructType, StringType, StructField,
                               DecimalType, DateType, ArrayType,
                               FloatType, TimestampType, BooleanType,
                               LongType, IntegerType, DoubleType, MapType)
from sparkly.hive_metastore_manager import _type_to_hql, _get_create_table_statement


class TestHqlCreateTableStatement(TestCase):

    def test_string(self):
        self.assertEqual(
            _type_to_hql(StringType().jsonValue()),
            'string',
        )

    def test_long(self):
        self.assertEqual(
            _type_to_hql(LongType().jsonValue()),
            'bigint',
        )

    def test_integer(self):
        self.assertEqual(
            _type_to_hql(IntegerType().jsonValue()),
            'int',
        )

    def test_double(self):
        self.assertEqual(
            _type_to_hql(DoubleType().jsonValue()),
            'double',
        )

    def test_boolean(self):
        self.assertEqual(
            _type_to_hql(BooleanType().jsonValue()),
            'boolean',
        )

    def test_timestamp(self):
        self.assertEqual(
            _type_to_hql(StructField('value', TimestampType(), True).jsonValue()),
            'timestamp',
        )

    def test_decimal(self):
        self.assertEqual(
            _type_to_hql(StructField('value', DecimalType(19, 4), True).jsonValue()),
            'decimal(19,4)',
        )

    def test_date(self):
        self.assertEqual(
            _type_to_hql(StructField('value', DateType(), False).jsonValue()),
            'date',
        )

    def test_array(self):
        self.assertEqual(
            _type_to_hql(ArrayType(DateType(), False).jsonValue()),
            'array<date>',
        )

    def test_map(self):
        self.assertEqual(
            _type_to_hql(
                MapType(StringType(), DateType()).jsonValue()
            ),
            'map<string,date>',
        )

    def test_struct(self):
        self.assertEqual(
            _type_to_hql(
                StructType([
                    StructField('name', StringType()),
                    StructField('age', DateType())
                ]).jsonValue()
            ),
            'struct<`name`:string,`age`:date>',
        )

    def test_nesting(self):
        self.assertEqual(
            _type_to_hql(
                StructType([
                    StructField(
                        'name',
                        StructType([
                            StructField(
                                'arr',
                                ArrayType(
                                    MapType(StringType(), DateType())
                                )
                            )
                        ])),
                ]).jsonValue()
            ),
            'struct<`name`:struct<`arr`:array<map<string,date>>>>',
        )

    def test_get_create_table_sql_partition_by(self):
        result = _get_create_table_statement(
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

    def test_type_to_sql_inner_struct(self):
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
        res = _get_create_table_statement('test',
                                          StructType([StructField("f1", StringType(), True)]),
                                          location='s3://test/')
        self.assertEqual(res,
                         "CREATE EXTERNAL TABLE `test` (`f1` string) "
                         "STORED AS PARQUET LOCATION 's3://test/'")

    def test_ci_topics_case(self):
        schema = ArrayType(
            ArrayType(
                StructType([
                    StructField('relevance_score', FloatType()),
                    StructField('title', StringType()),
                    StructField('topic_id', StringType()),
                ])
            )
        )
        self.assertEqual(_type_to_hql(schema.jsonValue()),
                         'array<array<struct<'
                         '`relevance_score`:float,'
                         '`title`:string,'
                         '`topic_id`:string'
                         '>>>')
