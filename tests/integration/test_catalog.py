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

from sparkly.testing import SparklyGlobalSessionTest
from tests.integration.base import SparklyTestSession


class TestSparklyCatalog(SparklyGlobalSessionTest):
    session = SparklyTestSession

    def setUp(self):
        self.spark.catalog_ext.drop_table('test_table')

        if self.spark.catalog_ext.has_database('test_db'):
            for table in self.spark.catalog.listTables('test_db'):
                self.spark.catalog_ext.drop_table('test_db.{}'.format(table.name))
            self.spark.sql('DROP DATABASE test_db')

        df = self.spark.createDataFrame([('row_1', 1), ('row_2', 2)], schema=('a', 'b'))
        df.write.saveAsTable('test_table', format='parquet', location='/tmp/test_table')

        self.spark.catalog_ext.set_table_property('test_table', 'property_a', 'str_value')
        self.spark.catalog_ext.set_table_property('test_table', 'property_b', 2)

        self.spark.sql('CREATE DATABASE test_db')
        df.write.saveAsTable('test_db.test_table', format='parquet', location='/tmp/test_table')
        self.spark.catalog_ext.set_table_property('test_db.test_table',
                                                  'property_a',
                                                  'str_value')
        self.spark.catalog_ext.set_table_property('test_db.test_table',
                                                  'property_b',
                                                  2)

    def test_has_database(self):
        self.assertTrue(self.spark.catalog_ext.has_database('test_db'))
        self.assertFalse(self.spark.catalog_ext.has_database('not_exists'))

    def test_create_table_when_exists(self):
        self.assertTrue(self.spark.catalog_ext.has_table('test_table'))

        new_df = self.spark.createDataFrame([('row_5', 'hi')], schema=('c', 'd'))
        new_df.write.save('/tmp/test_table_2', format='parquet', mode='overwrite')

        self.spark.catalog_ext.create_table(
            'test_table',
            path='/tmp/test_table_2',
            schema=new_df.schema,
            mode='overwrite',
        )

        self.assertTrue(self.spark.catalog_ext.has_table('test_table'))

        new_table = self.spark.table('test_table')
        self.assertEqual(
            [r.asDict() for r in new_table.collect()],
            [{'c': 'row_5', 'd': 'hi'}],
        )

    def test_drop_table(self):
        self.assertTrue(self.spark.catalog_ext.has_table('test_table'))

        self.spark.catalog_ext.drop_table('test_table')

        self.assertFalse(self.spark.catalog_ext.has_table('test_table'))

    def test_drop_table_non_default_db(self):
        self.assertTrue(self.spark.catalog_ext.has_table('test_db.test_table'))

        self.spark.catalog_ext.drop_table('test_db.test_table')

        self.assertFalse(self.spark.catalog_ext.has_table('test_db.test_table'))

    def test_has_table(self):
        self.assertTrue(self.spark.catalog_ext.has_table('test_table'))
        self.assertTrue(self.spark.catalog_ext.has_table('test_db.test_table'))
        self.assertFalse(self.spark.catalog_ext.has_table('test_unknown_table'))
        self.assertFalse(self.spark.catalog_ext.has_table('non_exists.test_unknown_table'))

    def test_rename_table(self):
        self.spark.catalog_ext.drop_table('new_test_table')
        self.assertTrue(self.spark.catalog_ext.has_table('test_table'))
        self.assertFalse(self.spark.catalog_ext.has_table('new_test_table'))

        self.spark.catalog_ext.rename_table('test_table', 'new_test_table')

        self.assertFalse(self.spark.catalog_ext.has_table('test_table'))
        self.assertTrue(self.spark.catalog_ext.has_table('new_test_table'))
        self.assertEqual(self.spark.table('new_test_table').count(), 2)

    def test_rename_table_non_default_db(self):
        self.spark.catalog_ext.drop_table('test_db.new_test_table')
        self.assertTrue(self.spark.catalog_ext.has_table('test_db.test_table'))
        self.assertFalse(self.spark.catalog_ext.has_table('test_db.new_test_table'))

        self.spark.catalog_ext.rename_table('test_db.test_table', 'new_test_table')

        self.assertFalse(self.spark.catalog_ext.has_table('test_db.test_table'))
        self.assertTrue(self.spark.catalog_ext.has_table('default.new_test_table'))
        self.assertEqual(self.spark.table('default.new_test_table').count(), 2)

    def test_get_table_properties(self):
        properties = self.spark.catalog_ext.get_table_properties('test_table')

        self.assertEqual(properties.get('property_a'), 'str_value')
        self.assertEqual(properties.get('property_b'), '2')

    def test_get_table_property(self):
        self.assertEqual(
            self.spark.catalog_ext.get_table_property('test_table', 'property_a'),
            'str_value',
        )
        self.assertEqual(
            self.spark.catalog_ext.get_table_property('test_db.test_table', 'property_a'),
            'str_value',
        )

    def test_get_table_property_to_type(self):
        self.assertEqual(
            self.spark.catalog_ext.get_table_property('test_table', 'property_b', to_type=int),
            2,
        )
        self.assertEqual(
            self.spark.catalog_ext.get_table_property('test_db.test_table',
                                                      'property_b',
                                                      to_type=int),
            2,
        )

    def test_get_table_property_unknown(self):
        self.assertIsNone(self.spark.catalog_ext.get_table_property('test_table', 'unknown'))
        self.assertIsNone(
            self.spark.catalog_ext.get_table_property('test_db.test_table', 'unknown')
        )
