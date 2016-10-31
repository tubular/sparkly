try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse

from sparkle.schema_parser import parse
from sparkle.utils import (
    config_reader_writer,
    to_parsed_url_and_options,
)


class SparkleReader(object):
    """A set of tools to create DataFrames from the external storages.

    Note:
        We don't expect you to be using the class directly.
        The instance of the class is available under `SparkleContext` via `read_ext` attribute.
    """
    def __init__(self, hc):
        self._hc = hc

    def by_url(self, url):
        """Create a dataframe using `url`.

        The main idea behind the method is to unify data access interface for different
        formats and locations. A generic schema looks like::

            format:[protocol:]//host[:port][/location][?configuration]

        Supported formats:

            - CSV ``csv://``
            - Cassandra ``cassandra://``
            - Elastic ``elastic://``
            - MySQL ``mysql://``
            - Parquet ``parquet://``
            - Hive Metastore table ``table://``

        Query string arguments are passed as parameters to the relevant reader.\n
        For instance, the next data source URL::

            cassandra://localhost:9042/my_keyspace/my_table?consistency=ONE
                &parallelism=3&spark.cassandra.connection.compression=LZ4

        Is an equivalent for::

            hc.read_ext.cassandra(
                host='localhost',
                port=9042,
                keyspace='my_keyspace',
                table='my_table',
                consistency='ONE',
                parallelism=3,
                options={'spark.cassandra.connection.compression': 'LZ4'},
            )

        More examples::

            table://table_name
            csv:s3://some-bucket/some_directory?header=true
            csv://path/on/local/file/system?header=false
            parquet:s3://some-bucket/some_directory
            elastic://elasticsearch.host/es_index/es_type?parallelism=8
            cassandra://cassandra.host/keyspace/table?consistency=QUORUM
            mysql://mysql.host/database/table

        Args:
            url (str): Data source URL.

        Returns:
            pyspark.sql.DataFrame
        """
        _by_url_registry = {
            'parquet': self._fs_resolver,
            'csv': self._csv_resolver,
            'cassandra': self._cassandra_resolver,
            'mysql': self._mysql_resolver,
            'elastic': self._elastic_resolver,
            'table': self._table_resolver,
        }

        scheme = urlparse(url).scheme
        try:
            return _by_url_registry[scheme](url)
        except KeyError:
            raise NotImplementedError('Data source is not supported: {}'.format(url))

    def cassandra(self, host, keyspace, table, consistency=None, parallelism=None, port=None,
                  options=None):
        """Create a dataframe from a Cassandra table.

        Args:
            host (str): Cassandra server host.
            keyspace (str) Cassandra keyspace to read from.
            table (str): Cassandra table to read from.
            consistency (str): Read consistency level: ``ONE``, ``QUORUM``, ``ALL``, etc.
            parallelism (int|None): The max number of parallel tasks that could be executed
                during the read stage (see :ref:`controlling-the-load`).
            port (int|None): Cassandra server port.
            options (dict[str,str]|None): Additional options for `org.apache.spark.sql.cassandra`
                format (see configuration for :ref:`cassandra`).

        Returns:
            pyspark.sql.DataFrame
        """
        assert self._hc.has_package('datastax:spark-cassandra-connector')

        default_options = {
            'spark_cassandra_connection_host': host,
            'keyspace': keyspace,
            'table': table,
        }

        if options:
            default_options.update(options)

        if consistency:
            default_options['spark_cassandra_input_consistency_level'] = consistency

        if port:
            default_options['spark_cassandra_connection_port'] = str(port)

        reader = config_reader_writer(
            self._hc.read.format('org.apache.spark.sql.cassandra'), default_options
        ).load()

        if parallelism:
            reader = reader.coalesce(parallelism)

        return reader

    def csv(self, path, custom_schema=None, header=True, parallelism=None, options=None):
        """Create a dataframe from a CSV file.

        Args:
            path (str): Path to the file or directory.
            custom_schema (pyspark.sql.types.DataType): Force custom schema.
            header (bool): The first row is a header.
            parallelism (int|None): The max number of parallel tasks that could be executed
                during the read stage (see :ref:`controlling-the-load`).
            options (dict[str,str]|None): Additional options for `com.databricks.spark.csv` format.
                (see configuration for :ref:`csv`).

        Returns:
            pyspark.sql.DataFrame
        """
        assert self._hc.has_package('com.databricks:spark-csv')

        reader = config_reader_writer(self._hc.read.format('com.databricks.spark.csv'), {
            'header': str(header).lower(),
            'inferSchema': 'false' if custom_schema else 'true',
        })

        if custom_schema:
            reader = reader.schema(custom_schema)

        reader = config_reader_writer(reader, options).load(path)

        if parallelism:
            reader = reader.coalesce(parallelism)

        return reader

    def elastic(self, host, es_index, es_type, query='', fields=None, parallelism=None, port=None,
                options=None):
        """Create a dataframe from an ElasticSearch index.

        Args:
            host (str): Elastic server host.
            es_index (str): Elastic index.
            es_type (str): Elastic type.
            query (str): Pre-filter es documents, e.g. '?q=views:>10'.
            fields (list[str]|None): Select only specified fields.
            parallelism (int|None): The max number of parallel tasks that could be executed
                during the read stage (see :ref:`controlling-the-load`).
            port (int|None) Elastic server port.
            options (dict[str,str]): Additional options for `org.elasticsearch.spark.sql` format
                (see configuration for :ref:`elastic`).

        Returns:
            pyspark.sql.DataFrame
        """
        assert self._hc.has_package('org.elasticsearch:elasticsearch-spark')

        reader = config_reader_writer(self._hc.read.format('org.elasticsearch.spark.sql'), {
            'es.nodes': host,
            'es.query': query,
            'es.read.metadata': 'true',
            'es.mapping.date.rich': 'false',
        })

        if fields:
            reader = reader.option('es.read.field.include', ','.join(fields))

        reader = config_reader_writer(reader, options).load('{}/{}'.format(es_index, es_type))

        if parallelism:
            reader = reader.coalesce(parallelism)

        return reader

    def mysql(self, host, database, table, parallelism=None, port=None, options=None):
        """Create a dataframe from a MySQL table.

        Should be usable for rds, aurora, etc.
        Options should include user and password.

        Args:
            host (str): MySQL server address.
            database (str): Database to connect to.
            table (str): Table to read rows from.
            parallelism (int|None): The max number of parallel tasks that could be executed
                during the read stage (see :ref:`controlling-the-load`).
            port (int|None): MySQL server port.
            options (dict[str,str]|None): Additional options for JDBC reader
                (see configuration for :ref:`mysql`).

        Returns:
            pyspark.sql.DataFrame
        """
        assert self._hc.has_jar('mysql-connector-java')

        reader = config_reader_writer(self._hc.read.format('jdbc'), {
            'url': 'jdbc:mysql://{host}{port}/{database}'.format(
                host=host,
                port=':{}'.format(port) if port else '',
                database=database,
            ),
            'driver': 'com.mysql.jdbc.Driver',
            'dbtable': table,
        })
        reader = config_reader_writer(reader, options).load()

        if parallelism:
            reader = reader.coalesce(parallelism)

        return reader

    def _cassandra_resolver(self, url):
        inp, options = to_parsed_url_and_options(url)

        _, keyspace, table = inp.path.split('/')
        kwargs = {
            'options': options
        }
        consistency = options.pop('consistency', None)
        if consistency:
            kwargs['consistency'] = consistency

        parallelism = options.pop('parallelism', None)
        if parallelism:
            kwargs['parallelism'] = int(parallelism)

        return self.cassandra(inp.netloc, keyspace, table, **kwargs)

    def _elastic_resolver(self, url):
        inp, options = to_parsed_url_and_options(url)
        host = inp.netloc
        q = options.pop('q', None)
        query = '?q={}'.format(q) if q else ''

        fields = options.pop('fields', None)
        if fields:
            fields = fields.split(',')

        parallelism = options.pop('parallelism', None)
        if parallelism:
            parallelism = int(parallelism)

        es_index, es_type = inp.path.lstrip('/').split('/', 1)
        return self.elastic(host, es_index, es_type,
                       query=query, fields=fields,
                       parallelism=parallelism, options=options)

    def _table_resolver(self, url):
        inp, options = to_parsed_url_and_options(url)
        df = self._hc.table(inp.netloc)

        parallelism = options.pop('parallelism', None)
        if parallelism:
            df = df.coalesce(int(parallelism))

        return df

    def _fs_resolver(self, url):
        inp, options = to_parsed_url_and_options(url)
        df = self._hc.read.format(inp.scheme).options(**options).load(inp.path)

        parallelism = options.pop('parallelism', None)
        if parallelism:
            df = df.coalesce(int(parallelism))

        return df

    def _mysql_resolver(self, url):
        inp, options = to_parsed_url_and_options(url)
        db, table = inp.path[1:].split('/', 1)

        kwargs = {
            'options': options,
        }

        parallelism = options.pop('parallelism', None)
        if parallelism:
            kwargs['parallelism'] = int(parallelism)

        return self.mysql(inp.netloc, db, table, **kwargs)

    def _csv_resolver(self, url):
        inp, options = to_parsed_url_and_options(url)
        kwargs = {}

        header = options.pop('header', None)
        if header is not None:
            kwargs['header'] = header == 'true'

        custom_schema = options.pop('custom_schema', None)
        if custom_schema:
            custom_schema = parse(custom_schema)
            kwargs['custom_schema'] = custom_schema

        if options:
            kwargs['options'] = options

        parallelism = options.pop('parallelism', None)
        if parallelism:
            kwargs['parallelism'] = int(parallelism)

        return self.csv(inp.path, **kwargs)
