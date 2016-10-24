from pyspark.sql.types import StringType

from sparkle import SparkleContext
from sparkle.utils import absolute_path


class _TestContext(SparkleContext):
    packages = [
        'com.databricks:spark-csv_2.10:1.4.0',
        'datastax:spark-cassandra-connector:1.6.1-s_2.10',
        'org.elasticsearch:elasticsearch-spark_2.10:2.3.0',
    ]

    jars = [
        absolute_path(__file__, 'resources', 'brickhouse-0.7.1.jar'),
        absolute_path(__file__, 'resources', 'mysql-connector-java-5.1.39-bin.jar'),
    ]

    udfs = {
        'collect_max': 'brickhouse.udf.collect.CollectMaxUDAF',
        'length_of_text': (lambda text: len(text), StringType())
    }
