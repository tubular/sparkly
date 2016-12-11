try:
    from urllib.parse import urlparse, parse_qsl
except ImportError:
    from urlparse import urlparse, parse_qsl

from kafka import KafkaProducer
from pyspark.sql import DataFrame
from pyspark.rdd import RDD


class SparklyWriter(object):
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

            csv:s3://some-s3-bucket/some-s3-key?partitionBy=date,platform
            cassandra://cassandra.host/keyspace/table?consistency=ONE&mode=append
            parquet:///var/log/?partitionBy=date
            elastic://elastic.host/es_index/es_type
            mysql://mysql.host/database/table

        Args:
            url (str): Destination URL.
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

    def cassandra(self, host, keyspace, table, consistency=None, port=None, mode=None,
                  parallelism=None, options=None):
        """Write a dataframe to a Cassandra table.

        Args:
            host (str): Cassandra server host.
            keyspace (str): Cassandra keyspace to write to.
            table (str): Cassandra table to write to.
            consistency (str|None): Write consistency level: ``ONE``, ``QUORUM``, ``ALL``, etc.
            port (int|None): Cassandra server port.
            mode (str|None): Spark save mode,
                http://spark.apache.org/docs/latest/sql-programming-guide.html#save-modes
            parallelism (int|None): The max number of parallel tasks that could be executed
                during the write stage (see :ref:`controlling-the-load`).
            options (dict[str, str]): Additional options to `org.apache.spark.sql.cassandra`
                format (see configuration for :ref:`cassandra`).
        """
        assert self._hc.has_package('datastax:spark-cassandra-connector')

        writer_options = {
            'format': 'org.apache.spark.sql.cassandra',
            'spark_cassandra_connection_host': host,
            'keyspace': keyspace,
            'table': table,
        }

        if consistency:
            writer_options['spark_cassandra_output_consistency_level'] = consistency

        if port:
            writer_options['spark_cassandra_connection_port'] = str(port)

        return self._basic_write(writer_options, options, parallelism, mode)

    def csv(self, path, header=False, mode=None, partitionBy=None, parallelism=None, options=None):
        """Write a dataframe to a CSV file.

        Args:
            path (str): Path to the output directory.
            header (bool): First row is a header.
            mode (str|None): Spark save mode,
                http://spark.apache.org/docs/latest/sql-programming-guide.html#save-modes
            partitionBy (list[str]): Names of partitioning columns.
            parallelism (int|None): The max number of parallel tasks that could be executed
                during the write stage (see :ref:`controlling-the-load`).
            options (dict[str, str]): Additional options to `com.databricks.spark.csv`
                format (see configuration for :ref:`csv`).
        """
        assert self._hc.has_package('com.databricks:spark-csv')

        writer_options = {
            'path': path,
            'format': 'com.databricks.spark.csv',
            'header': 'true' if header else 'false',
            'partitionBy': partitionBy,
        }

        return self._basic_write(writer_options, options, parallelism, mode)

    def elastic(self, host, es_index, es_type, port=None, mode=None,
                parallelism=None, options=None):
        """Write a dataframe into an ElasticSearch index.

        Args:
            host (str): Elastic server host.
            es_index (str): Elastic index.
            es_type (str): Elastic type.
            port (int|None) Elastic server port.
            mode (str|None): Spark save mode,
                http://spark.apache.org/docs/latest/sql-programming-guide.html#save-modes
            parallelism (int|None): The max number of parallel tasks that could be executed
                during the write stage (see :ref:`controlling-the-load`).
            options (dict[str, str]): Additional options to `org.elasticsearch.spark.sql` format
                (see configuration for :ref:`elastic`).
        """
        assert self._hc.has_package('org.elasticsearch:elasticsearch-spark')

        writer_options = {
            'path': '{}/{}'.format(es_index, es_type),
            'format': 'org.elasticsearch.spark.sql',
            'es.nodes': host,
        }

        if port:
            writer_options['es.port'] = str(port)

        return self._basic_write(writer_options, options, parallelism, mode)

    def mysql(self, host, database, table, port=None, mode=None, parallelism=None, options=None):
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
        assert self._hc.has_jar('mysql-connector-java')

        writer_options = {
            'format': 'jdbc',
            'driver': 'com.mysql.jdbc.Driver',
            'url': 'jdbc:mysql://{host}{port}/{database}'.format(
                host=host,
                port=':{}'.format(port) if port else '',
                database=database,
            ),
            'table': table,
        }

        return self._basic_write(writer_options, options, parallelism, mode)

    def kafka(self,
              brokers,
              topic,
              key_serializer,
              value_serializer,
              parallelism=None,
              options=None):
        """Writes dataframge to kafka topic.

        Expected schema is:
          key -> Struct(...)
          value -> Struct(...)

        Args:
            brokers (list[str]): List of kafka brokers.
            topic (str): Topic to write to.
            key_serializer (function): Function to serialize key.
            value_serializer (function): Function to serialize value.
            parallelism (int|None): The max number of parallel tasks that could be executed
                during the write stage (see :ref:`controlling-the-load`).
            options (dict|None): Additional options.
        """
        def dict_to_tuple(item):
            as_dict = item.asDict(recursive=True)
            return as_dict['key'], as_dict['value']

        rdd = self._df.rdd.map(dict_to_tuple)
        rdd.write_ext.kafka(brokers, topic, key_serializer, value_serializer, parallelism, options)

    def _basic_write(self, writer_options, additional_options, parallelism, mode):
        if mode:
            writer_options['mode'] = mode

        writer_options.update(additional_options or {})

        df = self._df
        if parallelism:
            df = df.coalesce(parallelism)

        # For some reason the native `df.write.jdbc` calls `_jwrite` directly
        # so we can't use `df.write.save` for it.
        if writer_options['format'] == 'jdbc':
            return df.write.jdbc(
                url=writer_options.pop('url'),
                table=writer_options.pop('table'),
                mode=writer_options.pop('mode', None),
                properties=writer_options,
            )
        else:
            return df.write.save(**writer_options)

    def _resolve_cassandra(self, parsed_url, parsed_qs):
        return self.cassandra(
            host=parsed_url.netloc,
            keyspace=parsed_url.path.split('/')[1],
            table=parsed_url.path.split('/')[2],
            consistency=parsed_qs.pop('consistency', None),
            port=parsed_url.port,
            mode=parsed_qs.pop('mode', None),
            parallelism=parsed_qs.pop('parallelism', None),
            options=parsed_qs,
        )

    def _resolve_csv(self, parsed_url, parsed_qs):
        kwargs = {}

        if 'header' in parsed_qs:
            kwargs['header'] = parsed_qs.pop('header') == 'true'

        if 'partitionBy' in parsed_qs:
            kwargs['partitionBy'] = parsed_qs.pop('partitionBy').split(',')

        return self.csv(
            path=parsed_url.path,
            mode=parsed_qs.pop('mode', None),
            parallelism=parsed_qs.pop('parallelism', None),
            options=parsed_qs,
            **kwargs
        )

    def _resolve_elastic(self, parsed_url, parsed_qs):
        return self.elastic(
            host=parsed_url.netloc,
            es_index=parsed_url.path.split('/')[1],
            es_type=parsed_url.path.split('/')[2],
            port=parsed_url.port,
            mode=parsed_qs.pop('mode', None),
            parallelism=parsed_qs.pop('parallelism', None),
            options=parsed_qs,
        )

    def _resolve_mysql(self, parsed_url, parsed_qs):
        return self.mysql(
            host=parsed_url.netloc,
            database=parsed_url.path.split('/')[1],
            table=parsed_url.path.split('/')[2],
            port=parsed_url.port,
            mode=parsed_qs.pop('mode', None),
            parallelism=parsed_qs.pop('parallelism', None),
            options=parsed_qs,
        )

    def _resolve_parquet(self, parsed_url, parsed_qs):
        parallelism = parsed_qs.pop('parallelism', None)
        if parallelism:
            df = self._df.coalesce(int(parallelism))
        else:
            df = self._df

        if 'partitionBy' in parsed_qs:
            parsed_qs['partitionBy'] = parsed_qs.pop('partitionBy').split(',')

        df.write.save(
            path=parsed_url.path,
            format='parquet',
            **parsed_qs
        )


class SparklyRDDWriter(object):

    def __init__(self, rdd):
        self._rdd = rdd

    def kafka(self,
              brokers,
              topic,
              key_serializer,
              value_serializer,
              parallelism=None,
              options=None):
        """Writes rdd to kafka.

        RDD items should be two-element tuples, containing key and value.

        Args:
            brokers (list[str]): List of kafka brokers.
            topic (str): Topic to write to.
            key_serializer (function): Function to serialize key.
            value_serializer (function): Function to serialize value.
            parallelism (int|None): The max number of parallel tasks that could be executed
                during the write stage (see :ref:`controlling-the-load`).
            options (dict|None): Additional options.
        """

        def _map(messages):
            producer = KafkaProducer(
                bootstrap_servers=brokers,
                key_serializer=key_serializer,
                value_serializer=value_serializer,
            )
            for message in messages:
                key, val = message
                producer.send(topic, key=key, value=val)

            producer.flush()
            producer.close()

            return messages

        rdd = self._rdd
        if parallelism:
            rdd = rdd.coalesce(parallelism)

        rdd.mapPartitions(_map).count()


def attach_writer_to_dataframe():
    """A tiny amount of magic to attach write extensions."""
    def write_ext(self):
        return SparklyWriter(self)

    def write_rdd_ext(self):
        return SparklyRDDWriter(self)

    DataFrame.write_ext = property(write_ext)
    RDD.write_ext = property(write_rdd_ext)
