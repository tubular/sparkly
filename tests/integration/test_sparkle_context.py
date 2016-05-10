from sparkle.test import SparkleGlobalContextTest
from tests.integration.base import _TestContext


class TestSparkleContext(SparkleGlobalContextTest):

    context = _TestContext

    def test_lambda_udf(self):
        res = self.hc.sql("""
            SELECT length_of_text('xxxx')
        """)

        self.assertEqual(res.collect()[0][0], '4')

    def test_udf_for_jar(self):
        res = self.hc.sql("""
                select collect_max(tt.site, tt.uid) from
                (select stack(4, 'A', 40,
                                 'A', 81,
                                 'B', 16,
                                 'B', 22) as (site, uid)) as tt

        """)

        self.assertEqual(res.collect()[0][0], {'A': 40, 'B': 16})
