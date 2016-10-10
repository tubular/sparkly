import uuid
import os

from sparkle.hql import (get_all_tables, create_table,
                         table_exists, set_table_property,
                         get_table_property,
                         get_all_table_properties,
                         replace_table)
from sparkle.test import SparkleTest
from tests.integration.base import _TestContext


class TestHql(SparkleTest):

    context = _TestContext

    @classmethod
    def setUpClass(cls):
        super(TestHql, cls).setUpClass()

        cls.base = '/tmp/sparkle/{}'.format(uuid.uuid4().hex)
        cls.path = '{}/test/'.format(cls.base)
        cls.df = cls.hc.createDataFrame([
            ('Pavlo', 26, '2016-01-01', 'youtube'), ('Johny', 30, '2016-01-01', 'youtube'),
            ('Carl', 69, '2016-01-01', 'facebook'), ('Jessica1', 16, '2016-01-02', 'facebook'),
            ('Jessica2', 15, '2016-01-02', 'facebook'), ('Jessica3', 17, '2016-01-02', 'facebook'),
        ], ['name', 'age', 'date', 'platform'])
        cls.df.write.parquet(cls.path, partitionBy=['platform', 'date'])
        create_table(
            cls.hc,
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

    def test_get_all_tables(self):
        self.assertEqual(get_all_tables(self.hc), ['test_table'])

    def test_table_exists_zero(self):
        self.assertFalse(table_exists(self.hc, 'non_existent_table'))
        self.assertTrue(table_exists(self.hc, 'test_table'))

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

        all_props = get_all_table_properties(self.hc, 'test_table')
        self.assertEqual(all_props['name'], 'johnny')
        self.assertEqual(all_props['surname'], 'cache')

    def test_set_get_table_property(self):
        set_table_property(self.hc, 'test_table', 'name', 'Johny')
        name = get_table_property(self.hc, 'test_table', 'name')
        self.assertEqual(name, 'Johny')

        set_table_property(self.hc, 'test_table', 'name', 'Cage')
        name = get_table_property(self.hc, 'test_table', 'name')
        self.assertEqual(name, 'Cage')

    def test_get_all_table_properties(self):
        set_table_property(self.hc, 'test_table', 'name', 'Johny')
        set_table_property(self.hc, 'test_table', 'surname', 'Cache')
        set_table_property(self.hc, 'test_table', 'age', 99)

        res = get_all_table_properties(self.hc, 'test_table')
        self.assertTrue(
            {'name', 'surname', 'age'}.issubset(set(res.keys()))
        )
        self.assertEqual(res['name'], 'Johny')
        self.assertEqual(res['surname'], 'Cache')
        self.assertEqual(res['age'], '99')

    def test_replace_table(self):
        old_path = '{}/old/'.format(self.base)
        df = self.hc.createDataFrame([
            ('Jess', 36, '2116-01-02', 'facebook'),
        ], ['name', 'age', 'date', 'platform'])
        df.write.parquet(old_path, partitionBy=['platform'])

        create_table(
            self.hc,
            'old_table',
            df,
            partition_by=['platform'],
            location=old_path,
            properties={
                'name': 'Johnny',
                'surname': 'Cache',
            }
        )

        old_props = get_all_table_properties(self.hc, 'old_table')

        res = self.hc.sql("""
            SELECT name, age, date, platform FROM old_table
        """).collect()

        self.assertEqual(set([(item[0], item[1], item[2], item[3]) for item in res]),
                         {('Jess', 36, '2116-01-02', 'facebook')})

        replace_table(self.hc, 'old_table', self.df,
                      location=self.path,
                      partition_by=['platform', 'date'],
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

        all_props = get_all_table_properties(self.hc, 'old_table')
        del all_props['last_modified_time']
        del all_props['transient_lastDdlTime']
        del old_props['last_modified_time']
        del old_props['transient_lastDdlTime']
        self.assertEqual(all_props, old_props)
