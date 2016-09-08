import os

from pyspark.sql.types import StringType
from sparkle.test import SparkleTest

from sparkle.utils import absolute_path
from sparkle import SparkleContext


class _TestContext(SparkleContext):

    packages = ['datastax:spark-cassandra-connector:1.5.0-M3-s_2.10',
                'org.elasticsearch:elasticsearch-spark_2.10:2.3.0',
                'com.databricks:spark-csv_2.10:1.4.0',
                'org.apache.spark:spark-streaming-kafka_2.10:1.6.1',
                ]

    jars = [
        absolute_path(__file__, 'resources', 'brickhouse-0.7.1.jar'),
        absolute_path(__file__, 'resources', 'mysql-connector-java-5.1.39-bin.jar'),
    ]

    udfs = {
        'collect_max': 'brickhouse.udf.collect.CollectMaxUDAF',
        'length_of_text': (lambda text: len(text), StringType())
    }


class BaseCassandraTest(SparkleTest):

    context = _TestContext
    cql_setup_files = []
    cql_teardown_files = []

    def setUp(self):
        super(BaseCassandraTest, self).setUp()
        self.c_host = 'cassandra.docker'
        self._setup_data()

    def tearDown(self):
        super(BaseCassandraTest, self).tearDown()
        self._clear_data()

    def _setup_data(self):
        for file_path in self.cql_setup_files:
            os.system(
                'source venv2/bin/activate && cqlsh -f {} {}'.format(
                    file_path,
                    self.c_host
                )
            )

    def _clear_data(self):
        for file_path in self.cql_teardown_files:
            os.system(
                'source venv2/bin/activate && cqlsh -f {} {}'.format(
                    file_path,
                    self.c_host
                )
            )


class BaseElasticTest(SparkleTest):

    context = _TestContext
    elastic_setup_files = []
    elastic_teardown_indexes = []

    def setUp(self):
        super(BaseElasticTest, self).setUp()
        self.es_host = 'elastic.docker'
        self._setup_data()

    def tearDown(self):
        super(BaseElasticTest, self).tearDown()
        self._clear_data()

    def _setup_data(self):
        for file_path in self.elastic_setup_files:
            os.system(
                'curl -XPOST \'http://{}:9200/_bulk\' --data-binary @{}'.format(
                    self.es_host,
                    file_path
                )
            )

    def _clear_data(self):
        for index in self.elastic_teardown_indexes:
            os.system('curl -XDELETE \'http://{}:9200/{}\''.format(
                index,
                self.es_host
            ))


class BaseMysqlTest(SparkleTest):

    context = _TestContext
    sql_setup_files = []
    sql_teardown_files = []

    def setUp(self):
        super(BaseMysqlTest, self).setUp()
        self.mysql_host = 'mysql.docker'
        self._setup_data()

    def tearDown(self):
        super(BaseMysqlTest, self).tearDown()
        self._clear_data()

    def _setup_data(self):
        for file_path in self.sql_setup_files:
            os.system('mysql -h{} -uroot < {}'.format(self.mysql_host, file_path))

    def _clear_data(self):
        for file_path in self.sql_teardown_files:
            os.system('mysql -h{} -uroot < {}'.format(self.mysql_host, file_path))
