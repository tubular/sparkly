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

from pyspark.sql import types as T

from sparkly.testing import SparklyTest as SparklyTestSuite


class SparklyTest(SparklyTestSuite):
    session = None

    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    # py27 complains that it doesn't find it...
    def runTest(self):
        pass


class TestAssertRowsEqual(unittest.TestCase):

    # shallow comparisons
    def test_dict(self):
        first = dict([('one', 1), ('two', 2), ('three', 3), ('four', 4)])
        second = dict([('three', 3), ('two', 2), ('four', 4), ('one', 1)])

        SparklyTest().assertRowsEqual(first, second, ignore_order=True)
        SparklyTest().assertRowsEqual(first, second, ignore_order=False)

        # change entry ('four', 4)
        second = dict([('three', 3), ('two', 2), ('four', 44), ('one', 1)])

        with self.assertRaises(AssertionError):
            SparklyTest().assertRowsEqual(first, second, ignore_order=True)

    def test_list(self):
        first = ['one', 'two', 'three', 'four']
        second = ['three', 'two', 'four', 'one']

        SparklyTest().assertRowsEqual(first, second, ignore_order=True)

        with self.assertRaises(AssertionError):
            SparklyTest().assertRowsEqual(first, second, ignore_order=False)

        # change entry 'four'
        second = ['three', 'two', 'fourrrr', 'one']

        with self.assertRaises(AssertionError):
            SparklyTest().assertRowsEqual(first, second, ignore_order=True)

    def test_datatype(self):
        first = T.StructType([
            T.StructField('f1', T.BooleanType()),
            T.StructField('f2', T.ByteType()),
            T.StructField('f3', T.IntegerType()),
            T.StructField('f4', T.LongType()),
        ])
        second = T.StructType([
            T.StructField('f3', T.IntegerType()),
            T.StructField('f2', T.ByteType()),
            T.StructField('f4', T.LongType()),
            T.StructField('f1', T.BooleanType()),
        ])

        SparklyTest().assertRowsEqual(first, second, ignore_order=True)
        with self.assertRaises(AssertionError):
            self.assertEqual(first, second)

        # change entry (f4, T.LongType)
        second = T.StructType([
            T.StructField('f3', T.IntegerType()),
            T.StructField('f2', T.ByteType()),
            T.StructField('f4', T.StringType()),
            T.StructField('f1', T.BooleanType()),
        ])

        with self.assertRaises(AssertionError):
            SparklyTest().assertRowsEqual(first, second, ignore_order=True)

    def test_float(self):
        with self.assertRaises(AssertionError):
            SparklyTest().assertRowsEqual(0.51, 0.5)

        # Absolute tolerance
        with self.assertRaises(AssertionError):
            SparklyTest().assertRowsEqual(0.51, 0.5, atol=0.009)
        SparklyTest().assertRowsEqual(0.51, 0.5, atol=0.01)

        # Relative tolerance
        with self.assertRaises(AssertionError):
            SparklyTest().assertRowsEqual(0.51, 0.5, rtol=0.019)
        SparklyTest().assertRowsEqual(0.51, 0.5, rtol=0.021)

        # Relative + absolute tolerance
        with self.assertRaises(AssertionError):
            SparklyTest().assertRowsEqual(0.51, 0.5, rtol=0.009, atol=0.004)
        SparklyTest().assertRowsEqual(0.51, 0.5, rtol=0.01, atol=0.006)

        # Equal Nans
        with self.assertRaises(AssertionError):
            SparklyTest().assertRowsEqual(float('NaN'), float('NaN'), equal_nan=False)
        SparklyTest().assertRowsEqual(float('NaN'), float('NaN'))

    # nested
    def test_row_with_rows_arrays_and_maps(self):
        first = [
            T.Row(
                one=1,
                two=T.Row(
                    array=[
                        T.Row(income=10, month='2017-01'),
                        T.Row(income=9, month='2017-02'),
                        T.Row(income=8, month='2017-03'),
                    ],
                    map={
                        '2017-01': [1, 11, 111],
                        '2017-02': [2, 22, 222],
                        '2017-03': [3, 33, 333],
                    },
                    row=T.Row(my_id=2, my_desired_id=4)
                ),
                three=None,
                four=None,
            ),
            T.Row(
                one=None,
                two=None,
                three=[{'2017-01': 201701}, {'2017-02': 201702}, {'2017-03': 201703}],
                four={
                    '2017-01': T.Row(income=1, job='engineer'),
                    '2017-02': T.Row(income=2, job='cto'),
                    '2017-03': T.Row(income=3, job='ceo'),
                },
            ),
        ]
        second = [
            T.Row(
                three=[{'2017-02': 201702}, {'2017-01': 201701}, {'2017-03': 201703}],
                two=None,
                four={
                    '2017-01': T.Row(job='engineer', income=1),
                    '2017-02': T.Row(income=2, job='cto'),
                    '2017-03': T.Row(job='ceo', income=3),
                },
                one=None,
            ),
            T.Row(
                two=T.Row(
                    map={
                        '2017-01': [11, 1, 111],
                        '2017-02': [22, 222, 2],
                        '2017-03': [3, 33, 333],
                    },
                    array=[
                        T.Row(month='2017-03', income=8),
                        T.Row(month='2017-02', income=9),
                        T.Row(month='2017-01', income=10),
                    ],
                    row=T.Row(my_desired_id=4, my_id=2)
                ),
                four=None,
                three=None,
                one=1,
            ),
        ]

        SparklyTest().assertRowsEqual(first, second, ignore_order=True)

        with self.assertRaises(AssertionError):
            SparklyTest().assertRowsEqual(first, second, ignore_order=False)
        with self.assertRaises(AssertionError):
            self.assertEqual(first, second)

        # let's mess that equality up
        second[1]['two']['map']['2017-01'] = None
        with self.assertRaises(AssertionError):
            SparklyTest().assertRowsEqual(first, second, ignore_order=True)

    def test_datatype_with_structs_arrays_and_maps(self):
        first = T.StructType([
            T.StructField('f1', T.BooleanType()),
            T.StructField(
                'f2',
                T.StructType([
                    T.StructField(
                        's1',
                        T.ArrayType(T.StructType([
                            T.StructField('ss1', T.BooleanType()),
                            T.StructField('ss2', T.ByteType())
                        ])),
                    ),
                ]),
            ),
            T.StructField('f3', T.ArrayType(T.MapType(T.StringType(), T.IntegerType()))),
            T.StructField(
                'f4',
                T.MapType(
                    T.StringType(),
                    T.StructType([
                        T.StructField('ss1', T.IntegerType()),
                        T.StructField('ss2', T.LongType()),
                    ])),
            ),
        ])
        second = T.StructType([
            T.StructField('f3', T.ArrayType(T.MapType(T.StringType(), T.IntegerType()))),
            T.StructField(
                'f2',
                T.StructType([
                    T.StructField(
                        's1',
                        T.ArrayType(T.StructType([
                            T.StructField('ss2', T.ByteType()),
                            T.StructField('ss1', T.BooleanType()),
                        ])),
                    ),
                ]),
            ),
            T.StructField(
                'f4',
                T.MapType(
                    T.StringType(),
                    T.StructType([
                        T.StructField('ss2', T.LongType()),
                        T.StructField('ss1', T.IntegerType()),
                    ])),
            ),
            T.StructField('f1', T.BooleanType()),
        ])

        SparklyTest().assertRowsEqual(first, second, ignore_order=True)
        with self.assertRaises(AssertionError):
            self.assertEqual(first, second)

        # change entry (f4.ss1, T.LongType)
        second = T.StructType([
            T.StructField('f3', T.ArrayType(T.MapType(T.StringType(), T.IntegerType()))),
            T.StructField(
                'f2',
                T.StructType([
                    T.StructField(
                        's1',
                        T.ArrayType(T.StructType([
                            T.StructField('ss2', T.ByteType()),
                            T.StructField('ss1', T.BooleanType()),
                        ])),
                    ),
                ]),
            ),
            T.StructField(
                'f4',
                T.MapType(
                    T.StringType(),
                    T.StructType([
                        T.StructField('ss2', T.LongType()),
                        T.StructField('ss1', T.LongType()),
                    ])),
            ),
            T.StructField('f1', T.BooleanType()),
        ])

        with self.assertRaises(AssertionError):
            SparklyTest().assertRowsEqual(first, second, ignore_order=True)

    def test_float_nested_in_tuples(self):
        first = {
            '2017-01': [2.6, 2.7, 2.8],
            '2017-02': [12.6, 12.7, 12.8],
        }
        second = {
            '2017-01': [2.4, 2.5, 2.6],
            '2017-02': [11.6, 11.7, 11.8],
        }

        SparklyTest().assertRowsEqual(first, second, rtol=0.1, ignore_order=True)
        with self.assertRaises(AssertionError):
            SparklyTest().assertRowsEqual(first, second, ignore_order=True)

    # various
    def test_asserters_do_not_persist_between_calls(self):
        testing_instance = SparklyTest()

        testing_instance.assertRowsEqual(0.5, 0.51, rtol=0.1)

        with self.assertRaises(AssertionError):
            testing_instance.assertEqual(0.5, 0.51)

    def test_ignore_order_depth(self):
        first = [
            [{'one': [1, 11]}, {'eleven': [11, 111]}],
            [{'two': [2, 22]}, {'twenty two': [22, 222]}],
            [{'three': [3, 33]}, {'thirty three': [33, 333]}],
        ]

        # depth 1 permutation
        second = [
            [{'two': [2, 22]}, {'twenty two': [22, 222]}],
            [{'one': [1, 11]}, {'eleven': [11, 111]}],
            [{'three': [3, 33]}, {'thirty three': [33, 333]}],
        ]

        SparklyTest().assertRowsEqual(first, second, ignore_order=True, ignore_order_depth=1)
        with self.assertRaises(AssertionError):
            SparklyTest().assertRowsEqual(first, second, ignore_order=False)

        # depth 2 permutation ('twenty two' <-> 'two')
        second = [
            [{'twenty two': [22, 222]}, {'two': [2, 22]}],
            [{'one': [1, 11]}, {'eleven': [11, 111]}],
            [{'three': [3, 33]}, {'thirty three': [33, 333]}],
        ]

        SparklyTest().assertRowsEqual(first, second, ignore_order=True, ignore_order_depth=2)
        with self.assertRaises(AssertionError):
            SparklyTest().assertRowsEqual(first, second, ignore_order=True, ignore_order_depth=1)
        with self.assertRaises(AssertionError):
            SparklyTest().assertRowsEqual(first, second, ignore_order=False)

        # as deep as we can get (22 <-> 222, 11 <-> 111)
        second = [
            [{'twenty two': [222, 22]}, {'two': [2, 22]}],
            [{'one': [1, 11]}, {'eleven': [111, 11]}],
            [{'three': [3, 33]}, {'thirty three': [33, 333]}],
        ]
        SparklyTest().assertRowsEqual(first, second, ignore_order=True, ignore_order_depth=3)
        SparklyTest().assertRowsEqual(first, second, ignore_order=True, ignore_order_depth=0)
        SparklyTest().assertRowsEqual(first, second, ignore_order=True, ignore_order_depth=-1)
        SparklyTest().assertRowsEqual(first, second, ignore_order=True)
        with self.assertRaises(AssertionError):
            SparklyTest().assertRowsEqual(first, second, ignore_order=True, ignore_order_depth=2)
