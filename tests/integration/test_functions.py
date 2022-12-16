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

from collections import OrderedDict
import operator

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import utils as U

from sparkly import functions as SF
from sparkly.testing import SparklyGlobalSessionTest
from tests.integration.base import SparklyTestSession


class TestMultiJoin(SparklyGlobalSessionTest):
    session = SparklyTestSession

    def test_no_dataframes_in_the_input(self):
        joined_df = SF.multijoin([])
        self.assertIsNone(joined_df)

    def test_inner_join(self):
        first_df = self.spark.createDataFrame(
            data=[(1, ), (2, ), (3, )],
            schema=T.StructType([T.StructField('id', T.IntegerType())]),
        )
        second_df = self.spark.createDataFrame(
            data=[(2, ), (3, ), (4, )],
            schema=T.StructType([T.StructField('id', T.IntegerType())]),
        )
        third_df = self.spark.createDataFrame(
            data=[(3, ), (4, ), (5, )],
            schema=T.StructType([T.StructField('id', T.IntegerType())]),
        )

        joined_df = SF.multijoin([first_df, second_df, third_df], on='id', how='inner')

        self.assertRowsEqual(joined_df.collect(), [{'id': 3}])

    def test_outer_join(self):
        first_df = self.spark.createDataFrame(
            data=[(1, ), (2, ), (3, )],
            schema=T.StructType([T.StructField('id', T.IntegerType())]),
        )
        second_df = self.spark.createDataFrame(
            data=[(2, ), (3, ), (4, )],
            schema=T.StructType([T.StructField('id', T.IntegerType())]),
        )
        third_df = self.spark.createDataFrame(
            data=[(3, ), (4, ), (5, )],
            schema=T.StructType([T.StructField('id', T.IntegerType())]),
        )

        joined_df = SF.multijoin([first_df, second_df, third_df], on='id', how='outer')

        self.assertRowsEqual(joined_df.collect(), [{'id': i} for i in [1, 2, 3, 4, 5]])

    def test_coalescing(self):
        first_df = self.spark.createDataFrame(
            data=[(1, None), (2, 'hi'), (3, None), (4, 'may')],
            schema=T.StructType([
                T.StructField('id', T.IntegerType()),
                T.StructField('value', T.StringType()),
            ]),
        )
        second_df = self.spark.createDataFrame(
            data=[(2, 'hey'), (3, 'you'), (4, None)],
            schema=T.StructType([
                T.StructField('id', T.IntegerType()),
                T.StructField('value', T.StringType()),
            ]),
        )

        joined_df = SF.multijoin([first_df, second_df], on='id', how='inner', coalesce=['value'])

        self.assertRowsEqual(
            joined_df.collect(),
            [{'id': 2, 'value': 'hi'}, {'id': 3, 'value': 'you'}, {'id': 4, 'value': 'may'}],
        )

    def test_coalescing_light_type_mismatch(self):
        first_df = self.spark.createDataFrame(
            data=[(1, None), (2, 'hi'), (3, None), (4, 'may')],
            schema=T.StructType([
                T.StructField('id', T.IntegerType()),
                T.StructField('value', T.StringType()),
            ]),
        )
        second_df = self.spark.createDataFrame(
            data=[(2, 2), (3, 3), (4, None)],
            schema=T.StructType([
                T.StructField('id', T.IntegerType()),
                T.StructField('value', T.IntegerType()),
            ]),
        )

        joined_df = SF.multijoin([first_df, second_df], on='id', how='inner', coalesce=['value'])

        self.assertRowsEqual(
            joined_df.collect(),
            [{'id': 2, 'value': 'hi'}, {'id': 3, 'value': '3'}, {'id': 4, 'value': 'may'}],
        )

    def test_coalescing_heavy_type_mismatch(self):
        first_df = self.spark.createDataFrame(
            data=[(1, None), (2, 'hi'), (3, None), (4, 'may')],
            schema=T.StructType([
                T.StructField('id', T.IntegerType()),
                T.StructField('value', T.StringType()),
            ]),
        )
        second_df = self.spark.createDataFrame(
            data=[(2, [2, ]), (3, [3, ]), (4, None)],
            schema=T.StructType([
                T.StructField('id', T.IntegerType()),
                T.StructField('value', T.ArrayType(T.IntegerType())),
            ]),
        )

        with self.assertRaises(U.AnalysisException):
            SF.multijoin([first_df, second_df], on='id', how='inner', coalesce=['value'])


class TestSwitchCase(SparklyGlobalSessionTest):
    session = SparklyTestSession

    def test_no_cases(self):
        df = self.spark.createDataFrame(
            data=[('one', ), ('two', ), ('three', ), ('hi', )],
            schema=T.StructType([T.StructField('name', T.StringType())]),
        )

        df = df.withColumn('value', SF.switch_case('name'))

        self.assertRowsEqual(
            df.collect(),
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

        self.assertRowsEqual(
            df.collect(),
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

        self.assertRowsEqual(
            df.collect(),
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

        self.assertRowsEqual(
            df.collect(),
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

        self.assertRowsEqual(
            df.collect(),
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

        self.assertRowsEqual(
            df.collect(),
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

        self.assertRowsEqual(
            df.collect(),
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

        self.assertRowsEqual(
            df.collect(),
            [
                {'value': 1, 'value_2': 11},
                {'value': 2, 'value_2': 4},
                {'value': 3, 'value_2': 3},
                {'value': 0, 'value_2': 0},
            ],
        )

    def test_switch_case_with_custom_operand_between(self):
        df = self.spark.createDataFrame(
            data=[(1, ), (2, ), (3, ), (0, )],
            schema=T.StructType([T.StructField('value', T.IntegerType())]),
        )

        df = df.withColumn(
            'value_2',
            SF.switch_case(
                'value',
                {
                    (1, 1): 'aloha',
                    (2, 3): 'hi',
                },
                operand=lambda c, v: c.between(*v),
            ),
        )

        self.assertRowsEqual(
            df.collect(),
            [
                {'value': 1, 'value_2': 'aloha'},
                {'value': 2, 'value_2': 'hi'},
                {'value': 3, 'value_2': 'hi'},
                {'value': 0, 'value_2': None},
            ],
        )

    def test_switch_case_with_custom_operand_lt(self):
        df = self.spark.createDataFrame(
            data=[(1, ), (2, ), (3, ), (0, )],
            schema=T.StructType([T.StructField('value', T.IntegerType())]),
        )

        df = df.withColumn(
            'value_2',
            SF.switch_case(
                'value',
                OrderedDict([
                    (1, 'worst'),
                    (2, 'bad'),
                    (3, 'good'),
                    (4, 'best'),
                ]),
                operand=operator.lt,
            ),
        )

        self.assertRowsEqual(
            df.collect(),
            [
                {'value': 1, 'value_2': 'bad'},
                {'value': 2, 'value_2': 'good'},
                {'value': 3, 'value_2': 'best'},
                {'value': 0, 'value_2': 'worst'},
            ],
        )

class TestArgmax(SparklyGlobalSessionTest):
    session = SparklyTestSession

    def test_non_nullable_values(self):
        df = self.spark.createDataFrame(
            data=[
                ('1', 'test1', None, 3),
                ('1', None, 2, 4),
                ('2', 'test2', 3, 1),
                ('2', 'test3', 4, 2),
            ],
            schema=T.StructType([
                T.StructField('id', T.StringType(), nullable=True),
                T.StructField('value1', T.StringType(), nullable=True),
                T.StructField('value2', T.IntegerType(), nullable=True),
                T.StructField('target', T.IntegerType(), nullable=True),
            ]),
        )

        df = (
            df
            .groupBy('id')
            .agg(
                F.max('target').alias('target'),
                *[
                    SF.argmax(col, 'target', condition=F.col(col).isNotNull()).alias(col)
                    for col in df.columns
                    if col not in ['id', 'target']
                ]
            )
        )

        self.assertRowsEqual(
            df.collect(),
            [
                {'id': '1', 'target': 4, 'value1': 'test1', 'value2': 2},
                {'id': '2', 'target': 2, 'value1': 'test3', 'value2': 4},
            ],
        )

    def test_nullable_values(self):
        df = self.spark.createDataFrame(
            data=[
                ('1', 'test1', None, 3),
                ('1', None, 2, 4),
                ('2', 'test2', 3, 1),
                ('2', 'test3', 4, 2),
            ],
            schema=T.StructType([
                T.StructField('id', T.StringType(), nullable=True),
                T.StructField('value1', T.StringType(), nullable=True),
                T.StructField('value2', T.IntegerType(), nullable=True),
                T.StructField('target', T.IntegerType(), nullable=True),
            ]),
        )

        df = (
            df
            .groupBy('id')
            .agg(
                F.max('target').alias('target'),
                *[
                    SF.argmax(col, 'target').alias(col)
                    for col in df.columns
                    if col not in ['id', 'target']
                ]
            )
        )

        self.assertRowsEqual(
            df.collect(),
            [
                {'id': '1', 'target': 4, 'value1': None, 'value2': 2},
                {'id': '2', 'target': 2, 'value1': 'test3', 'value2': 4},
            ],
        )

    def test_break_ties(self):
        df = self.spark.createDataFrame(
            data=[
                ('1', 'test1', 1, 4),
                ('1', 'test2', 1, 3),
                ('2', 'test3', 1, 4),
                ('2', 'test4', 2, 3),
            ],
            schema=T.StructType([
                T.StructField('id', T.StringType(), nullable=True),
                T.StructField('value', T.StringType(), nullable=True),
                T.StructField('target1', T.IntegerType(), nullable=True),
                T.StructField('target2', T.IntegerType(), nullable=True),
            ]),
        )

        df = (
            df
            .groupBy('id')
            .agg(
                SF.argmax('value', ['target1', 'target2']).alias('value')
            )
        )

        self.assertRowsEqual(
            df.collect(),
            [
                {'id': '1', 'value': 'test1'},
                {'id': '2', 'value': 'test4'},
            ],
        )

    def test_with_conditions(self):
        df = self.spark.createDataFrame(
            data=[
                ('1', 'test1', 2),
                ('1', 'test2', 1),
                ('2', 'test3', 1),
                ('2', 'test4', 2),
            ],
            schema=T.StructType([
                T.StructField('id', T.StringType(), nullable=True),
                T.StructField('value', T.StringType(), nullable=True),
                T.StructField('target1', T.IntegerType(), nullable=True),
            ]),
        )

        df = (
            df
            .groupBy('id')
            .agg(
                SF.argmax(
                    'value',
                    'target1',
                    condition=F.col('value') != 'test1',
                ).alias('value'),
            )
        )

        self.assertRowsEqual(
            df.collect(),
            [
                {'id': '1', 'value': 'test2'},
                {'id': '2', 'value': 'test4'},
            ],
        )

    def test_with_column_expressions(self):
        df = self.spark.createDataFrame(
            data=[
                ('1', None, 'test1', 1, 4),
                ('1', 'test2', 'test2_1', 1, 3),
                ('2', 'test3', None, 1, 4),
                ('2', 'test4', 'test5', 2, 6),
            ],
            schema=T.StructType([
                T.StructField('id', T.StringType(), nullable=True),
                T.StructField('value1', T.StringType(), nullable=True),
                T.StructField('value2', T.StringType(), nullable=True),
                T.StructField('target1', T.IntegerType(), nullable=True),
                T.StructField('target2', T.IntegerType(), nullable=True),
            ]),
        )

        df = (
            df
            .groupBy('id')
            .agg(
                SF.argmax(
                    F.coalesce(F.col('value1'), F.col('value2')),
                    F.col('target1') + F.col('target2'),
                ).alias('value'),
            )
        )

        self.assertRowsEqual(
            df.collect(),
            [
                {'id': '1', 'value': 'test1'},
                {'id': '2', 'value': 'test4'},
            ],
        )
