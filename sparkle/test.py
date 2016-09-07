import os
from unittest import TestCase
import shutil

from sparkle import SparkleContext


class SparkleTest(TestCase):
    context = SparkleContext

    @classmethod
    def setUpClass(cls):
        super(SparkleTest, cls).setUpClass()
        cls.hc = cls.context()

    @classmethod
    def tearDownClass(cls):
        cls.hc._sc.stop()
        super(SparkleTest, cls).tearDownClass()

        try:
            shutil.rmtree('metastore_db')
        except OSError:
            pass

        try:
            os.unlink('derby.log')
        except OSError:
            pass

    def assertDataframeEqual(self, df, data, fields):
        """Check equality to dataframe contents.

        Args:
            df (pyspark.sql.Dataframe)
            data (list[tuple]): data to compare with
        """
        df_data = sorted([[x[y] for y in fields] for x in df.collect()])
        data = sorted(data)
        for df_row, data_row in zip(df_data, data):
            if len(df_row) != len(data_row):
                raise AssertionError('Rows have different length '
                                     'dataframe row: {}, Data row: {}'.format(df_row, data_row))

            for df_field, data_field in zip(df_row, data_row):
                if df_field != data_field:
                    raise AssertionError('{} != {}. Rows: dataframe - {}, data - {}'.format(
                        df_field, data_field, df_row, data_row
                    ))
