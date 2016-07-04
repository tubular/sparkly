import uuid
import os

from sparkle import SparkleContext
from sparkle.hql import (get_all_tables, create_table,
                         table_exists, set_table_property,
                         get_table_property)
from sparkle.test import SparkleTest


class _TestContext(SparkleContext):
    packages = ['datastax:spark-cassandra-connector:1.5.0-M3-s_2.10',
                'org.elasticsearch:elasticsearch-spark_2.10:2.3.0',
                'com.databricks:spark-csv_2.10:1.4.0',
                ]


class TestHql(SparkleTest):

    context = _TestContext

    @classmethod
    def setUpClass(cls):
        super(TestHql, cls).setUpClass()

        cls.s3_base = 's3a://tubular-tests/sparkle/{}/'.format(uuid.uuid4().hex)
        cls.df = cls.hc.createDataFrame([
            ('Pavlo', 26, '2016-01-01', 'youtube'), ('Johny', 30, '2016-01-01', 'youtube'),
            ('Carl', 69, '2016-01-01', 'facebook'), ('Jessica1', 16, '2016-01-02', 'facebook'),
            ('Jessica2', 15, '2016-01-02', 'facebook'), ('Jessica3', 17, '2016-01-02', 'facebook'),
        ], ['name', 'age', 'date', 'platform'])
        cls.df.write.parquet(cls.s3_base, partitionBy=['platform', 'date'])
        create_table(
            cls.hc,
            'test_table',
            cls.df,
            partition_by=['platform', 'date'],
            location=cls.s3_base,
        )

    @classmethod
    def tearDownClass(cls):
        os.system('aws s3 rm --recursive {}'.format(cls.s3_base))
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

    def test_set_get_table_property(self):
        set_table_property(self.hc, 'test_table', 'name', 'Johny')
        name = get_table_property(self.hc, 'test_table', 'name')
        self.assertEqual(name, 'Johny')

        set_table_property(self.hc, 'test_table', 'name', 'Cage')
        name = get_table_property(self.hc, 'test_table', 'name')
        self.assertEqual(name, 'Cage')
