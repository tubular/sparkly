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
try:
    from unittest import mock
except ImportError:
    import mock

from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql import types as T

from sparkly.utils import lru_cache, parse_schema, schema_has


class TestParseSchema(TestCase):
    def test_atomic(self):
        self.assert_parsed_properly('date')
        self.assert_parsed_properly('float')
        self.assert_parsed_properly('string')
        self.assert_parsed_properly('timestamp')
        self.assert_parsed_properly('int')

    def test_array(self):
        self.assert_parsed_properly('array<int>')

    def test_map(self):
        self.assert_parsed_properly('map<string,float>')

    def test_struct(self):
        self.assert_parsed_properly('struct<a:string,b:float>')

    def test_parse_complex_types(self):
        self.assert_parsed_properly('array<map<string,float>>')
        self.assert_parsed_properly('map<string,struct<a:map<bigint,string>>>')
        self.assert_parsed_properly('struct<a:struct<a:string>>')
        self.assert_parsed_properly('struct<a:map<bigint,map<string,int>>,c:map<int,string>>')

    def assert_parsed_properly(self, schema):
        self.assertEqual(parse_schema(schema).simpleString(), schema)


class TestLruCache(TestCase):
    def test_caching(self):
        df = mock.MagicMock(spec=DataFrame)

        called = [0]

        @lru_cache(storage_level=StorageLevel.DISK_ONLY)
        def func(*args, **kwargs):
            called[0] += 1
            return df

        func()
        df.persist.assert_called_once_with(StorageLevel.DISK_ONLY)
        self.assertEqual(df.unpersist.mock_calls, [])
        self.assertEqual(called[0], 1)

        cached_df = func()
        self.assertEqual(cached_df, df)
        self.assertEqual(called[0], 1)

    def test_eviction(self):
        first_df = mock.MagicMock(spec=DataFrame)
        second_df = mock.MagicMock(spec=DataFrame)

        @lru_cache(maxsize=1, storage_level=StorageLevel.DISK_ONLY)
        def func(uid):
            if uid == 'first':
                return first_df
            else:
                return second_df

        func('first')
        first_df.persist.assert_called_once_with(StorageLevel.DISK_ONLY)
        self.assertEqual(first_df.unpersist.mock_calls, [])

        func('second')
        first_df.persist.assert_called_once_with(StorageLevel.DISK_ONLY)
        first_df.unpersist.assert_called_once_with()
        second_df.persist.assert_called_once_with(StorageLevel.DISK_ONLY)
        self.assertEqual(second_df.unpersist.mock_calls, [])


class TestSchemaHas(TestCase):
    def test_structs_equal(self):
        schema_has(
            T.StructType([
                T.StructField('f1', T.IntegerType()),
                T.StructField('f2', T.FloatType()),
                T.StructField('f3', T.StringType()),
            ]),
            T.StructType([
                T.StructField('f3', T.StringType()),
                T.StructField('f2', T.FloatType()),
                T.StructField('f1', T.IntegerType()),
            ]),
        )

    def test_structs_equal_with_dict(self):
        schema_has(
            T.StructType([
                T.StructField('f1', T.IntegerType()),
                T.StructField('f2', T.FloatType()),
                T.StructField('f3', T.StringType()),
            ]),
            {
                'f1': T.IntegerType(),
                'f2': T.FloatType(),
                'f3': T.StringType(),
            },
        )

    def test_structs_subset(self):
        schema_has(
            T.StructType([
                T.StructField('f1', T.IntegerType()),
                T.StructField('f2', T.FloatType()),
                T.StructField('f3', T.StringType()),
            ]),
            T.StructType([
                T.StructField('f2', T.FloatType()),
            ]),
        )

    def test_structs_nested_subset(self):
        schema_has(
            T.StructType([
                T.StructField(
                    'f1',
                    T.ArrayType(T.StructType([
                        T.StructField('f11', T.IntegerType()),
                        T.StructField('f12', T.StringType()),
                    ])),
                ),
            ]),
            T.StructType([
                T.StructField(
                    'f1',
                    T.ArrayType(T.StructType([T.StructField('f11', T.IntegerType())])),
                ),
            ]),
        )

    def test_arrays_equal(self):
        schema_has(
            T.ArrayType(T.ArrayType(T.ArrayType(T.LongType()))),
            T.ArrayType(T.ArrayType(T.ArrayType(T.LongType()))),
        )

    def test_arrays_nested_subset(self):
        schema_has(
            T.ArrayType(T.ArrayType(T.StructType([
                T.StructField('f1', T.ArrayType(T.LongType())),
                T.StructField('f2', T.ArrayType(T.StringType())),
            ]))),
            T.ArrayType(T.ArrayType(T.StructType([
                T.StructField('f1', T.ArrayType(T.LongType()))
            ]))),
        )

    def test_maps_equal(self):
        schema_has(
            T.MapType(T.StringType(), T.MapType(T.StringType(), T.LongType())),
            T.MapType(T.StringType(), T.MapType(T.StringType(), T.LongType())),
        )

    def test_maps_nested_subset(self):
        schema_has(
            T.MapType(
                T.StringType(),
                T.MapType(
                    T.StringType(),
                    T.StructType([
                        T.StructField('f1', T.MapType(T.StringType(), T.LongType())),
                        T.StructField('f2', T.MapType(T.StringType(), T.IntegerType())),
                    ]),
                ),
            ),
            T.MapType(
                T.StringType(),
                T.MapType(
                    T.StringType(),
                    T.StructType([
                        T.StructField('f1', T.MapType(T.StringType(), T.LongType())),
                    ]),
                ),
            ),
        )

    def test_type_mismatch(self):
        with self.assertRaisesRegex(AssertionError, 'Cannot compare heterogeneous types'):
            schema_has(
                T.StructType([T.StructField('f1', T.IntegerType())]),
                T.ArrayType(T.IntegerType()),
            )

        with self.assertRaisesRegex(AssertionError, 'Cannot compare heterogeneous types'):
            schema_has(
                T.ArrayType(T.IntegerType()),
                {'f1': T.IntegerType()},
            )

        with self.assertRaisesRegex(TypeError, r'f1 is IntegerType\(\), expected LongType\(\)'):
            schema_has(
                T.StructType([T.StructField('f1', T.IntegerType())]),
                T.StructType([T.StructField('f1', T.LongType())]),
            )

        with self.assertRaisesRegex(
            TypeError,
            r'f1\.element\.s1 is IntegerType\(\), expected LongType\(\)',
        ):
            schema_has(
                T.StructType([
                    T.StructField(
                        'f1',
                        T.ArrayType(T.StructType([T.StructField('s1', T.IntegerType())])),
                    ),
                ]),
                T.StructType([
                    T.StructField(
                        'f1',
                        T.ArrayType(T.StructType([T.StructField('s1', T.LongType())])),
                    ),
                ]),
            )

        with self.assertRaisesRegex(
            TypeError,
            r'element is IntegerType\(\), expected LongType\(\)',
        ):
            schema_has(
                T.ArrayType(T.IntegerType()),
                T.ArrayType(T.LongType()),
            )

        with self.assertRaisesRegex(TypeError, r'key is StringType\(\), expected LongType\(\)'):
            schema_has(
                T.MapType(T.StringType(), T.IntegerType()),
                T.MapType(T.LongType(), T.IntegerType()),
            )

        with self.assertRaisesRegex(TypeError, r'value is IntegerType\(\), expected LongType\(\)'):
            schema_has(
                T.MapType(T.StringType(), T.IntegerType()),
                T.MapType(T.StringType(), T.LongType()),
            )

    def test_undefined_field(self):
        with self.assertRaisesRegex(KeyError, 'f2'):
            schema_has(
                T.StructType([T.StructField('f1', T.IntegerType())]),
                T.StructType([T.StructField('f2', T.LongType())]),
            )

        with self.assertRaisesRegex(KeyError, r'f1\.element\.s2'):
            schema_has(
                T.StructType([
                    T.StructField(
                        'f1',
                        T.ArrayType(T.StructType([T.StructField('s1', T.IntegerType())])),
                    ),
                ]),
                T.StructType([
                    T.StructField(
                        'f1',
                        T.ArrayType(T.StructType([T.StructField('s2', T.LongType())])),
                    ),
                ]),
            )

        with self.assertRaisesRegex(
            TypeError,
            r'element is IntegerType\(\), expected LongType\(\)',
        ):
            schema_has(
                T.ArrayType(T.IntegerType()),
                T.ArrayType(T.LongType()),
            )
