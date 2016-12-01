import uuid
import os

import pytest

from sparkly.testing import SparklyGlobalContextTest
from tests.integration.base import _TestContext


class TestHql(SparklyGlobalContextTest):

    context = _TestContext

    @classmethod
    def setUpClass(cls):
        super(TestHql, cls).setUpClass()
        cls.base = '/tmp/sparkly/{}'.format(uuid.uuid4().hex)
        cls.path = '{}/test/'.format(cls.base)
        cls.df = cls.hc.createDataFrame([
            ('Pavlo', 26, '2016-01-01', 'youtube'), ('Johny', 30, '2016-01-01', 'youtube'),
            ('Carl', 69, '2016-01-01', 'facebook'), ('Jessica1', 16, '2016-01-02', 'facebook'),
            ('Jessica2', 15, '2016-01-02', 'facebook'), ('Jessica3', 17, '2016-01-02', 'facebook'),
        ], ['name', 'age', 'date', 'platform'])
        cls.df.write.parquet(cls.path, partitionBy=['platform', 'date'])
        cls.hc.sql('drop table if exists test_table')
        cls.hc.hms.create_table(
            'test_table',
            cls.df,
            partition_by=['platform', 'date'],
            location=cls.path,
            properties={
                'name': 'johnny',
                'surname': 'cache',
            }
        )

    @classmethod
    def tearDownClass(cls):
        os.system('rm -Rf {}'.format(cls.base))
        super(TestHql, cls).tearDownClass()

    def test_get_all_table_properties(self):
        table_manager = self.hc.hms.table('test_table')

        table_manager.set_property('name', 'Johny')
        table_manager.set_property('surname', 'Cache')
        table_manager.set_property('age', 99)

        res = table_manager.get_all_properties()
        self.assertTrue(
            {'name', 'surname', 'age'}.issubset(set(res.keys()))
        )
        self.assertEqual(res['name'], 'Johny')
        self.assertEqual(res['surname'], 'Cache')
        self.assertEqual(res['age'], '99')

    def test_get_all_tables(self):
        all_tables = self.hc.hms.get_all_tables()
        self.assertIsInstance(all_tables, list)
        self.assertIn('test_table', all_tables)

    def test_table_exists(self):
        self.assertTrue(self.hc.hms.table('test_table').exists())
        self.assertFalse(self.hc.hms.table('not_test_table').exists())

    def test_set_property(self):
        self.assertEqual(
            self.hc.hms.table('test_table').set_property('xxx', 'yyy').get_property('xxx'),
            'yyy'
        )

    def test_create_table(self):
        res = self.hc.sql("""
            SELECT * FROM test_table
        """).coalesce(1).collect()

        self.assertEqual({(x.name, x.age, x.date, x.platform) for x in res}, {
            ('Pavlo', 26, '2016-01-01', 'youtube'),
            ('Johny', 30, '2016-01-01', 'youtube'),
            ('Carl', 69, '2016-01-01', 'facebook'),
            ('Jessica1', 16, '2016-01-02', 'facebook'),
            ('Jessica2', 15, '2016-01-02', 'facebook'),
            ('Jessica3', 17, '2016-01-02', 'facebook'),
        })

        all_props = self.hc.hms.table('test_table').get_all_properties()
        self.assertEqual(all_props['name'], 'johnny')
        self.assertEqual(all_props['surname'], 'cache')

    def test_replace_table(self):
        old_path = '{}/old/'.format(self.base)
        df = self.hc.createDataFrame([
            ('Jess', 36, '2116-01-02', 'facebook'),
        ], ['name', 'age', 'date', 'platform'])
        df.write.parquet(old_path, partitionBy=['platform'])

        self.hc.sql('drop table if exists old_table')
        self.hc.hms.create_table(
            'old_table',
            df,
            partition_by=['platform'],
            location=old_path,
            properties={
                'name': 'Johnny',
                'surname': 'Cache',
            }
        )

        old_props = self.hc.hms.table('old_table').get_all_properties()

        res = self.hc.sql("""
            SELECT name, age, date, platform FROM old_table
        """).collect()

        self.assertEqual(set([(item[0], item[1], item[2], item[3]) for item in res]),
                         {('Jess', 36, '2116-01-02', 'facebook')})

        self.hc.hms.replace_table(
            'old_table', self.df,
            location=self.path, partition_by=['platform', 'date'],
        )

        res2 = self.hc.sql("""
            SELECT name, age, date, platform FROM old_table
        """).collect()

        self.assertEqual(
            set([(item[0], item[1], item[2], item[3]) for item in res2]),
            {
                ('Carl', 69, '2016-01-01', 'facebook'),
                ('Pavlo', 26, '2016-01-01', 'youtube'),
                ('Jessica3', 17, '2016-01-02', 'facebook'),
                ('Jessica2', 15, '2016-01-02', 'facebook'),
                ('Jessica1', 16, '2016-01-02', 'facebook'),
                ('Johny', 30, '2016-01-01', 'youtube'),
            }
        )

        all_props = self.hc.hms.table('old_table').get_all_properties()
        del all_props['last_modified_time']
        del all_props['transient_lastDdlTime']
        del old_props['last_modified_time']
        del old_props['transient_lastDdlTime']
        self.assertEqual(all_props, old_props)
