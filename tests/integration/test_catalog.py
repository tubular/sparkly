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

import uuid
import os

from sparkly.testing import SparklyGlobalSessionTest
from tests.integration.base import _TestSession


class TestSparklyCatalog(SparklyGlobalSessionTest):
    session = _TestSession

    def setUp(self):
        self.spark.catalog_ext.drop_table('default.test_table')

        df = self.spark.createDataFrame([('row_1', 1), ('row_2', 2)], schema=('a', 'b'))
        df.write.saveAsTable('test_table', format='parquet', location='/tmp/test_table')

        self.spark.catalog_ext.set_table_property('test_table', 'property_a', 'str_value')
        self.spark.catalog_ext.set_table_property('test_table', 'property_b', 2)

    def test_drop_table(self):
        self.assertTrue(self.spark.catalog_ext.has_table('test_table'))

        self.spark.catalog_ext.drop_table('test_table')

        self.assertFalse(self.spark.catalog_ext.has_table('test_table'))

    def test_has_table(self):
        self.assertTrue(self.spark.catalog_ext.has_table('test_table'))
        self.assertTrue(self.spark.catalog_ext.has_table('default.test_table'))
        self.assertFalse(self.spark.catalog_ext.has_table('test_unknown_table'))

    def test_rename_table(self):
        self.spark.catalog_ext.drop_table('new_test_table')
        self.assertTrue(self.spark.catalog_ext.has_table('test_table'))
        self.assertFalse(self.spark.catalog_ext.has_table('new_test_table'))

        self.spark.catalog_ext.rename_table('test_table', 'default.new_test_table')

        self.assertFalse(self.spark.catalog_ext.has_table('test_table'))
        self.assertTrue(self.spark.catalog_ext.has_table('new_test_table'))
        self.assertEqual(self.spark.table('new_test_table').count(), 2)

    def test_get_table_properties(self):
        properties = self.spark.catalog_ext.get_table_properties('test_table')

        self.assertEqual(properties.get('property_a'), 'str_value')
        self.assertEqual(properties.get('property_b'), '2')

    def test_get_table_property(self):
        self.assertEqual(
            self.spark.catalog_ext.get_table_property('test_table', 'property_a'),
            'str_value',
        )

    def test_get_table_property_to_type(self):
        self.assertEqual(
            self.spark.catalog_ext.get_table_property('test_table', 'property_b', to_type=int),
            2,
        )

    def test_get_table_property_unknown(self):
        self.assertIsNone(self.spark.catalog_ext.get_table_property('test_table', 'unknown'))
