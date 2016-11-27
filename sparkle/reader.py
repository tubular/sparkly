try:
    from urllib.parse import urlparse, parse_qsl
except ImportError:
    from urlparse import urlparse, parse_qsl

from sparkle import schema_parser


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
        parsed_url = urlparse(url)
        parsed_qs = dict(parse_qsl(parsed_url.query))

        # Used across all readers
        if 'parallelism' in parsed_qs:
            parsed_qs['parallelism'] = int(parsed_qs['parallelism'])

        try:
            resolver = getattr(self, '_resolve_{}'.format(parsed_url.scheme))
        except AttributeError:
            raise NotImplementedError('Data source is not supported: {}'.format(url))
        else:
            return resolver(parsed_url, parsed_qs)

    def cassandra(self, host, keyspace, table, consistency=None, port=None,
                  parallelism=None, options=None):
        """Create a dataframe from a Cassandra table.

        Args:
            host (str): Cassandra server host.
            keyspace (str) Cassandra keyspace to read from.
            table (str): Cassandra table to read from.
            consistency (str): Read consistency level: ``ONE``, ``QUORUM``, ``ALL``, etc.
            port (int|None): Cassandra server port.
            parallelism (int|None): The max number of parallel tasks that could be executed
                during the read stage (see :ref:`controlling-the-load`).
            options (dict[str,str]|None): Additional options for `org.apache.spark.sql.cassandra`
                format (see configuration for :ref:`cassandra`).

        Returns:
            pyspark.sql.DataFrame
        """
        assert self._hc.has_package('datastax:spark-cassandra-connector')

        reader_options = {
            'format': 'org.apache.spark.sql.cassandra',
            'spark_cassandra_connection_host': host,
            'keyspace': keyspace,
            'table': table,
        }

        if consistency:
            reader_options['spark_cassandra_input_consistency_level'] = consistency

        if port:
            reader_options['spark_cassandra_connection_port'] = str(port)

        return self._basic_read(reader_options, options, parallelism)

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

        reader_options = {
            'path': path,
            'format': 'com.databricks.spark.csv',
            'schema': custom_schema,
            'header': 'true' if header else 'false',
            'inferSchema': 'false' if custom_schema else 'true',
        }

        return self._basic_read(reader_options, options, parallelism)

    def elastic(self, host, es_index, es_type, query='', fields=None, port=None,
                parallelism=None, options=None):
        """Create a dataframe from an ElasticSearch index.

        Args:
            host (str): Elastic server host.
            es_index (str): Elastic index.
            es_type (str): Elastic type.
            query (str): Pre-filter es documents, e.g. '?q=views:>10'.
            fields (list[str]|None): Select only specified fields.
            port (int|None) Elastic server port.
            parallelism (int|None): The max number of parallel tasks that could be executed
                during the read stage (see :ref:`controlling-the-load`).
            options (dict[str,str]): Additional options for `org.elasticsearch.spark.sql` format
                (see configuration for :ref:`elastic`).

        Returns:
            pyspark.sql.DataFrame
        """
        assert self._hc.has_package('org.elasticsearch:elasticsearch-spark')

        reader_options = {
            'path': '{}/{}'.format(es_index, es_type),
            'format': 'org.elasticsearch.spark.sql',
            'es.nodes': host,
            'es.query': query,
            'es.read.metadata': 'true',
        }

        if fields:
            reader_options['es.read.field.include'] = ','.join(fields)

        if port:
            reader_options['es.port'] = str(port)

        return self._basic_read(reader_options, options, parallelism)

    def mysql(self, host, database, table, port=None, parallelism=None, options=None):
        """Create a dataframe from a MySQL table.

        Should be usable for rds, aurora, etc.
        Options should include user and password.

        Args:
            host (str): MySQL server address.
            database (str): Database to connect to.
            table (str): Table to read rows from.
            port (int|None): MySQL server port.
            parallelism (int|None): The max number of parallel tasks that could be executed
                during the read stage (see :ref:`controlling-the-load`).
            options (dict[str,str]|None): Additional options for JDBC reader
                (see configuration for :ref:`mysql`).

        Returns:
            pyspark.sql.DataFrame
        """
        assert self._hc.has_jar('mysql-connector-java')

        reader_options = {
            'format': 'jdbc',
            'driver': 'com.mysql.jdbc.Driver',
            'url': 'jdbc:mysql://{host}{port}/{database}'.format(
                host=host,
                port=':{}'.format(port) if port else '',
                database=database,
            ),
            'dbtable': table,
        }

        return self._basic_read(reader_options, options, parallelism)

    def _basic_read(self, reader_options, additional_options, parallelism):
        reader_options.update(additional_options or {})

        df = self._hc.read.load(**reader_options)
        if parallelism:
            df = df.coalesce(parallelism)

        return df

    def _resolve_cassandra(self, parsed_url, parsed_qs):
        return self.cassandra(
            host=parsed_url.netloc,
            keyspace=parsed_url.path.split('/')[1],
            table=parsed_url.path.split('/')[2],
            consistency=parsed_qs.pop('consistency', None),
            port=parsed_url.port,
            parallelism=parsed_qs.pop('parallelism', None),
            options=parsed_qs,
        )

    def _resolve_csv(self, parsed_url, parsed_qs):
        kwargs = {}

        if 'custom_schema' in parsed_qs:
            kwargs['custom_schema'] = schema_parser.parse(parsed_qs.pop('custom_schema'))

        if 'header' in parsed_qs:
            kwargs['header'] = parsed_qs.pop('header') == 'true'

        return self.csv(
            path=parsed_url.path,
            parallelism=parsed_qs.pop('parallelism', None),
            options=parsed_qs,
            **kwargs
        )

    def _resolve_elastic(self, parsed_url, parsed_qs):
        kwargs = {}

        if 'q' in parsed_qs:
            kwargs['query'] = '?q={}'.format(parsed_qs.pop('q'))

        if 'fields' in parsed_qs:
            kwargs['fields'] = parsed_qs.pop('fields').split(',')

        return self.elastic(
            host=parsed_url.netloc,
            es_index=parsed_url.path.split('/')[1],
            es_type=parsed_url.path.split('/')[2],
            port=parsed_url.port,
            parallelism=parsed_qs.pop('parallelism', None),
            options=parsed_qs,
            **kwargs
        )

    def _resolve_mysql(self, parsed_url, parsed_qs):
        return self.mysql(
            host=parsed_url.netloc,
            database=parsed_url.path.split('/')[1],
            table=parsed_url.path.split('/')[2],
            port=parsed_url.port,
            parallelism=parsed_qs.pop('parallelism', None),
            options=parsed_qs,
        )

    def _resolve_parquet(self, parsed_url, parsed_qs):
        parallelism = parsed_qs.pop('parallelism', None)

        df = self._hc.read.load(
            path=parsed_url.path,
            format=parsed_url.scheme,
            **parsed_qs,
        )

        if parallelism:
            df = df.coalesce(int(parallelism))

        return df

    def _resolve_table(self, parsed_url, parsed_qs):
        df = self._hc.table(parsed_url.netloc)

        parallelism = parsed_qs.pop('parallelism', None)
        if parallelism:
            df = df.coalesce(int(parallelism))

        return df
