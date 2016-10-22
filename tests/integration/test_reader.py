import unittest

import pytest


@pytest.mark.branch_1_0
class SparkleReaderTest(unittest.TestCase):
    def test_fake(self):
        self.assertEqual(1, 1)
