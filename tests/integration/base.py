from sparkle import SparkleContext


class _TestContext(SparkleContext):

    packages = ['datastax:spark-cassandra-connector:1.5.0-M3-s_2.10',
                'org.elasticsearch:elasticsearch-spark_2.10:2.3.0',
                'com.databricks:spark-csv_2.10:1.4.0',
                'org.apache.spark:spark-streaming-kafka_2.10:1.6.1',
                ]
