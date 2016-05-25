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
