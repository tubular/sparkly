from pyspark.sql.types import StringType

from sparkle import SparkleContext
from sparkle.test import SparkleTest


class _TestContext(SparkleContext):

    udfs = {
        'collect_max': 'brickhouse.udf.collect.CollectMaxUDAF',
        'length_of_text': (lambda text: len(text), StringType())
    }


class TestSparkleContext(SparkleTest):

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
