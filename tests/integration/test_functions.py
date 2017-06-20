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

from pyspark.sql import functions as F
from pyspark.sql import types as T

from sparkly import functions as SF
from sparkly.testing import SparklyGlobalSessionTest
from tests.integration.base import SparklyTestSession


class TestSwitchCase(SparklyGlobalSessionTest):
    session = SparklyTestSession

    def test_no_cases(self):
        df = self.spark.createDataFrame(
            data=[('one', ), ('two', ), ('three', ), ('hi', )],
            schema=T.StructType([T.StructField('name', T.StringType())]),
        )

        df = df.withColumn('value', SF.switch_case('name'))

        self.assertDataFrameEqual(
            df,
            [
                {'name': 'one', 'value': None},
                {'name': 'two', 'value': None},
                {'name': 'three', 'value': None},
                {'name': 'hi', 'value': None},
            ],
        )

    def test_default_as_a_lit(self):
        df = self.spark.createDataFrame(
            data=[('one', ), ('two', ), ('three', ), ('hi', )],
            schema=T.StructType([T.StructField('name', T.StringType())]),
        )

        df = df.withColumn('value', SF.switch_case('name', default=0))

        self.assertDataFrameEqual(
            df,
            [
                {'name': 'one', 'value': 0},
                {'name': 'two', 'value': 0},
                {'name': 'three', 'value': 0},
                {'name': 'hi', 'value': 0},
            ],
        )

    def test_default_as_a_column(self):
        df = self.spark.createDataFrame(
            data=[('one', ), ('two', ), ('three', ), ('hi', )],
            schema=T.StructType([T.StructField('name', T.StringType())]),
        )

        df = df.withColumn('value', SF.switch_case('name', default=F.col('name')))

        self.assertDataFrameEqual(
            df,
            [
                {'name': 'one', 'value': 'one'},
                {'name': 'two', 'value': 'two'},
                {'name': 'three', 'value': 'three'},
                {'name': 'hi', 'value': 'hi'},
            ],
        )

    def test_switch_as_a_string_cases_as_kwargs(self):
        df = self.spark.createDataFrame(
            data=[('one', ), ('two', ), ('three', ), ('hi', )],
            schema=T.StructType([T.StructField('name', T.StringType())]),
        )

        df = df.withColumn('value', SF.switch_case('name', one=1, two=2, three=3, default=0))

        self.assertDataFrameEqual(
            df,
            [
                {'name': 'one', 'value': 1},
                {'name': 'two', 'value': 2},
                {'name': 'three', 'value': 3},
                {'name': 'hi', 'value': 0},
            ],
        )

    def test_switch_as_a_column_cases_as_kwargs(self):
        df = self.spark.createDataFrame(
            data=[('one', ), ('two', ), ('three', ), ('hi', )],
            schema=T.StructType([T.StructField('name', T.StringType())]),
        )

        df = df.withColumn(
            'value',
            SF.switch_case(F.col('name'), one=1, two=2, three=3, default=0),
        )

        self.assertDataFrameEqual(
            df,
            [
                {'name': 'one', 'value': 1},
                {'name': 'two', 'value': 2},
                {'name': 'three', 'value': 3},
                {'name': 'hi', 'value': 0},
            ],
        )

    def test_dict_cases_override_kwarg_cases(self):
        df = self.spark.createDataFrame(
            data=[('one', ), ('two', ), ('three', ), ('hi', )],
            schema=T.StructType([T.StructField('name', T.StringType())]),
        )

        df = df.withColumn(
            'value',
            SF.switch_case('name', {'one': 11, 'three': 33}, one=1, two=2, three=3, default=0),
        )

        self.assertDataFrameEqual(
            df,
            [
                {'name': 'one', 'value': 11},
                {'name': 'two', 'value': 2},
                {'name': 'three', 'value': 33},
                {'name': 'hi', 'value': 0},
            ],
        )

    def test_cases_condition_constant_as_an_arbitrary_value(self):
        df = self.spark.createDataFrame(
            data=[(1, ), (2, ), (3, ), (0, )],
            schema=T.StructType([T.StructField('value', T.IntegerType())]),
        )

        df = df.withColumn(
            'name',
            SF.switch_case('value', {1: 'one', 2: 'two', 3: 'three'}, default='hi'),
        )

        self.assertDataFrameEqual(
            df,
            [
                {'name': 'one', 'value': 1},
                {'name': 'two', 'value': 2},
                {'name': 'three', 'value': 3},
                {'name': 'hi', 'value': 0},
            ],
        )

    def test_cases_values_as_a_column(self):
        df = self.spark.createDataFrame(
            data=[(1, ), (2, ), (3, ), (0, )],
            schema=T.StructType([T.StructField('value', T.IntegerType())]),
        )

        df = df.withColumn(
            'value_2',
            SF.switch_case(
                'value',
                {
                    1: 11 * F.col('value'),
                    2: F.col('value') * F.col('value'),
                    'hi': 5,
                },
                default=F.col('value'),
            ),
        )

        self.assertDataFrameEqual(
            df,
            [
                {'value': 1, 'value_2': 11},
                {'value': 2, 'value_2': 4},
                {'value': 3, 'value_2': 3},
                {'value': 0, 'value_2': 0},
            ],
        )
