from unittest import TestCase

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
