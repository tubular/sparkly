import pytest

from sparkle.test import SparkleGlobalContextTest
from tests.integration.base import _TestContext


class TestSparkleContext(SparkleGlobalContextTest):
    context = _TestContext

    def test_python_udf(self):
        rows = self.hc.sql('select length_of_text("hello world")')
        self.assertEqual(rows.collect()[0][0], '11')

    def test_jar_udf(self):
        self.hc.createDataFrame(
            [
                {'key_field': 'A', 'value_field': 1},
                {'key_field': 'B', 'value_field': 2},
                {'key_field': 'C', 'value_field': 3},
                {'key_field': 'D', 'value_field': 4},
            ],
        ).registerTempTable('test_jar_udf')

        rows = self.hc.sql('select collect_max(key_field, value_field, 2) from test_jar_udf')
        self.assertEqual(rows.collect()[0][0], {'C': 3, 'D': 4})
