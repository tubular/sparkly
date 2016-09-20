import os
from unittest import TestCase
import shutil

from sparkle import SparkleContext


class SparkleTest(TestCase):
    """Base test for spark scrip tests.

    Initializes and shuts down Context specified in `context` param.

    Example:

           >>> class MyTestCase(SparkleTest):
           >>>      def test(self):
           >>>          self.assertDataframeEqual(
           >>>              self.hc.sql('SELECT 1 as one').collect(),
           >>>              (1,), ['one']
           >>>          )

    """

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


class BaseCassandraTest(SparkleTest):
    """Base test class for Cassandra integration tests.

    Notes:
        * assumes `cqlsh` available in runtime environment.
        * cqlsh is currently only available for py2 so there is venv hack (you can override it).

    Examples:

           >>> class MyTestCase(BaseCassandraTest):
           >>>
           >>>      cql_setup_files = [absolute_path(__file__, 'resources', 'setup.cql')]
           >>>      cql_setup_files = [absolute_path(__file__, 'resources', 'teardown.cql')]
           >>>
           >>>      def test(self):
           >>>          pass

    """
    cql_setup_files = []
    cql_teardown_files = []

    cqlsh_execute_cmd = 'source venv2/bin/activate && cqlsh'
    c_host = 'cassandra.docker'  # Cassandra host to operate on

    def setUp(self):
        super(BaseCassandraTest, self).setUp()
        self._setup_data()

    def tearDown(self):
        super(BaseCassandraTest, self).tearDown()
        self._clear_data()

    def _setup_data(self):
        for file_path in self.cql_setup_files:
            os.system(
                '{} -f {} {}'.format(
                    self.cqlsh_execute_cmd,
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
    """Base test class for elastic integration tests.

    Notes: assumes `curl` available in runtime environment.

    Examples:

           >>> class MyTestCase(BaseElasticTest):
           >>>
           >>>      elastic_host = 'test.elastic.host.net'
           >>>      elastic_setup_files = [absolute_path(__file__, 'resources', 'setup.json')]
           >>>      elastic_teardown_indexes = ['my_test_index']
           >>>
           >>>      def test(self):
           >>>          pass

    """

    elastic_setup_files = []
    elastic_teardown_indexes = []
    elastic_host = 'elastic.docker'

    def setUp(self):
        super(BaseElasticTest, self).setUp()
        self._setup_data()

    def tearDown(self):
        super(BaseElasticTest, self).tearDown()
        self._clear_data()

    def _setup_data(self):
        for file_path in self.elastic_setup_files:
            os.system(
                'curl -XPOST \'http://{}:9200/_bulk\' --data-binary @{}'.format(
                    self.elastic_host,
                    file_path
                )
            )

    def _clear_data(self):
        for index in self.elastic_teardown_indexes:
            os.system('curl -XDELETE \'http://{}:9200/{}\''.format(
                index,
                self.elastic_host,
            ))


class BaseMysqlTest(SparkleTest):
    """Base test class for mysql integration tests.

    Notes: assumes mysql cli available in runtime environment.

    Examples:

           >>> class MyTestCase(BaseElasticTest):
           >>>
           >>>      mysql_host = 'test.mysql.host.net'
           >>>      sql_setup_files = [absolute_path(__file__, 'resources', 'setup.sql')]
           >>>      sql_teardown_files = [absolute_path(__file__, 'resources', 'teardown.sql')]
           >>>      mysql_user = 'root'
           >>>      mysql_password = 'root'
           >>>
           >>>      def test(self):
           >>>          pass
    """

    sql_setup_files = []
    sql_teardown_files = []
    mysql_host = 'mysql.docker'
    mysql_user = 'root'
    mysql_password = None

    def setUp(self):
        super(BaseMysqlTest, self).setUp()
        self._setup_data()

    def tearDown(self):
        super(BaseMysqlTest, self).tearDown()
        self._clear_data()

    def _setup_data(self):
        for file_path in self.sql_setup_files:
            if self.mysql_password:
                os.system('mysql -h{} -u{} -p{} < {}'.format(self.mysql_host,
                                                             self.mysql_user,
                                                             self.mysql_password,
                                                             file_path,
                                                             ))
            else:
                os.system('mysql -h{} -u{} < {}'.format(self.mysql_host,
                                                        self.mysql_user,
                                                        file_path,
                                                        ))

    def _clear_data(self):
        for file_path in self.sql_teardown_files:
            if self.mysql_password:
                os.system('mysql -h{} -u{} -p{} < {}'.format(self.mysql_host,
                                                             self.mysql_user,
                                                             self.mysql_password,
                                                             file_path,
                                                             ))
            else:
                os.system('mysql -h{} -u{} < {}'.format(self.mysql_host,
                                                        self.mysql_user,
                                                        file_path,
                                                        ))
