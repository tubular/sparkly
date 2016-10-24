try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse

from pyspark.sql import DataFrame

from sparkle.utils import (
    context_has_package,
    config_reader_writer,
    context_has_jar,
    to_parsed_url_and_options,
)


class SparkleWriter(object):
    """A set of tools to write DataFrames to the external storages.

    Note:
        We don't expect you to be using the class directly.
        The instance of the class is available under `DataFrame` via `write_ext` attribute.
    """
    def __init__(self, df):
        self._df = df
        self._hc = df.sql_ctx

    def by_url(self, url):
        """Write a dataframe to a destination specified by `url`.

        The main idea behind the method is to unify data export interface for different
        formats and locations. A generic schema looks like::

            format:[protocol:]//host[:port][/location][?configuration]

        Supported formats:

            - CSV ``csv://``
            - Cassandra ``cassandra://``
            - Elastic ``elastic://``
            - MySQL ``mysql://``
            - Parquet ``parquet://``

        Query string arguments are passed as parameters to the relevant writer.\n
        For instance, the next data export URL::

            elastic://localhost:9200/my_index/my_type?&parallelism=3&mode=overwrite
                &es.write.operation=upsert

        Is an equivalent for::

            hc.read_ext.elastic(
                host='localhost',
                port=9200,
                es_index='my_index',
                es_type='my_type',
                parallelism=3,
                mode='overwrite',
                options={'es.write.operation': 'upsert'},
            )

        More examples::

            csv:s3://some-s3-bucket/some-s3-key?partition_by=date,platform
            cassandra://cassandra.host/keyspace/table?consistency=ONE&mode=append
            parquet:///var/log/?partition_by=date
            elastic://elastic.host/es_index/es_type
            mysql://mysql.host/database/table

        Args:
            url (str): Destination URL.
        """
        _by_url_registry = {
            'parquet': self._fs_resolver,
            'csv': self._fs_resolver,
            'cassandra': self._cassandra_resolver,
            'mysql': self._mysql_resolver,
            'elastic': self._elastic_resolver,
        }
        scheme = urlparse(url).scheme
        try:
            return _by_url_registry[scheme](url)
        except KeyError:
            raise NotImplementedError(
                'Destination specified in url is not supported: {}'.format(url))

    def cassandra(self, host, keyspace, table, consistency=None, mode=None, parallelism=None,
                  options=None):
        """Write a dataframe to a Cassandra table.

        Args:
            host (str): Cassandra server host.
            keyspace (str): Cassandra keyspace to write to.
            table (str): Cassandra table to write to.
            consistency (str|None): Write consistency level: ``ONE``, ``QUORUM``, ``ALL``, etc.
            mode (str|None): Spark save mode,
                http://spark.apache.org/docs/latest/sql-programming-guide.html#save-modes
            parallelism (int|None): The max number of parallel tasks that could be executed
                during the write stage (see :ref:`controlling-the-load`).
            options (dict[str, str]): Additional options to `org.apache.spark.sql.cassandra`
                format (see configuration for :ref:`cassandra`).
        """
        assert context_has_package(self._hc, 'datastax:spark-cassandra-connector')

        default_options = {
            'spark_cassandra_connection_host': host,
            'keyspace': keyspace,
            'table': table,
        }

        if options:
            default_options.update(options)

        if consistency:
            default_options['spark_cassandra_output_consistency_level'] = consistency

        if parallelism:
            df = self._df.coalesce(parallelism)
        else:
            df = self._df

        config_reader_writer(df.write.format('org.apache.spark.sql.cassandra'), default_options)\
            .mode(mode).save()

    def csv(self, path, header=False, mode=None, parallelism=None, options=None):
        """Write a dataframe to a CSV file.

        Args:
            path (str): Path to the output directory.
            header (bool): First row is a header.
            mode (str|None): Spark save mode,
                http://spark.apache.org/docs/latest/sql-programming-guide.html#save-modes
            parallelism (int|None): The max number of parallel tasks that could be executed
                during the write stage (see :ref:`controlling-the-load`).
            options (dict[str, str]): Additional options to `com.databricks.spark.csv`
                format (see configuration for :ref:`csv`).
        """
        assert context_has_package(self._hc, 'com.databricks:spark-csv')

        if parallelism:
            df = self._df.coalesce(parallelism)
        else:
            df = self._df

        writer = config_reader_writer(df.write.format('com.databricks.spark.csv'), {
            'header': str(header).lower(),
        })

        config_reader_writer(writer, options).mode(mode).save(path)

    def elastic(self, host, es_index, es_type, mode=None, parallelism=None, options=None):
        """Write a dataframe into an ElasticSearch index.

        Args:
            host (str): Elastic server host.
            es_index (str): Elastic index.
            es_type (str): Elastic type.
            mode (str|None): Spark save mode,
                http://spark.apache.org/docs/latest/sql-programming-guide.html#save-modes
            parallelism (int|None): The max number of parallel tasks that could be executed
                during the write stage (see :ref:`controlling-the-load`).
            options (dict[str, str]): Additional options to `org.elasticsearch.spark.sql` format
                (see configuration for :ref:`elastic`).
        """
        assert context_has_package(self._hc, 'org.elasticsearch:elasticsearch-spark')

        if parallelism:
            df = self._df.coalesce(parallelism)
        else:
            df = self._df

        writer = config_reader_writer(df.write.format('org.elasticsearch.spark.sql'), {
            'es.nodes': host,
        })

        config_reader_writer(writer, options). \
            mode(mode). \
            save('{}/{}'.format(es_index, es_type))

    def mysql(self, host, database, table, mode=None, parallelism=None, port=None, options=None):
        """Write a dataframe to a MySQL table.

        Should be usable for rds, aurora, etc.
        Options should include user and password.

        Args:
            host (str): MySQL server address.
            database (str): Database to connect to.
            table (str): Table to read rows from.
            mode (str|None): Spark save mode,
                http://spark.apache.org/docs/latest/sql-programming-guide.html#save-modes
            parallelism (int|None): The max number of parallel tasks that could be executed
                during the write stage (see :ref:`controlling-the-load`).
            options (dict): Additional options for JDBC writer
                (see configuration for :ref:`mysql`).
        """
        assert context_has_jar(self._hc, 'mysql-connector-java')

        default_options = {
            'driver': 'com.mysql.jdbc.Driver'
        }

        if options:
            default_options.update(options)

        if parallelism:
            df = self._df.coalesce(parallelism)
        else:
            df = self._df

        df.write.jdbc(
            url='jdbc:mysql://{host}{port}/{database}'.format(
                host=host,
                port=':{}'.format(port) if port else '',
                database=database,
            ),
            table=table,
            mode=mode,
            properties=default_options,
        )

    def _fs_resolver(self, url):
        inp, options = to_parsed_url_and_options(url)
        partition_by = options.pop('partition_by', None)
        output_format = inp.scheme
        mode = options.pop('mode', None)
        parallelism = options.pop('parallelism', None)

        if parallelism:
            df = self._df.coalesce(int(parallelism))
        else:
            df = self._df

        df.write.save(
            path=inp.path,
            format=output_format,
            mode=mode,
            partitionBy=partition_by.split(',') if partition_by else None,
            **options,
        )

    def _cassandra_resolver(self, url):
        inp, options = to_parsed_url_and_options(url)
        _, db, table = inp.path.split('/')
        mode = options.pop('mode', None)
        consistency = options.pop('consistency', None)
        parallelism = options.pop('parallelism', None)
        return self.cassandra(
            host=inp.netloc,
            keyspace=db,
            table=table,
            mode=mode,
            consistency=consistency,
            parallelism=int(parallelism) if parallelism else None,
            options=options,
        )

    def _mysql_resolver(self, url):
        inp, options = to_parsed_url_and_options(url)
        _, db, table = inp.path.split('/')
        mode = options.pop('mode', None)
        parallelism = options.pop('parallelism', None)
        return self.mysql(
            host=inp.netloc,
            database=db,
            table=table,
            mode=mode,
            parallelism=int(parallelism) if parallelism else None,
            options=options,
        )

    def _elastic_resolver(self, url):
        inp, options = to_parsed_url_and_options(url)
        _, index, type_ = inp.path.split('/')
        mode = options.pop('mode', None)
        parallelism = options.pop('parallelism', None)
        return self.elastic(
            host=inp.netloc,
            es_index=index,
            es_type=type_,
            mode=mode,
            parallelism=int(parallelism) if parallelism else None,
            options=options,
        )


def monkey_patch_dataframe():
    """A tiny amount of magic to attach `SparkleWriter` to a `DataFrame`."""
    def write_ext(self):
        return SparkleWriter(self)

    DataFrame.write_ext = property(write_ext)
