#
# Copyright 2017 Tubular Labs, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

try:
    from urllib.parse import urlparse, parse_qsl
except ImportError:
    from urlparse import urlparse, parse_qsl

try:
    from kafka import KafkaProducer
except ImportError:
    KAFKA_WRITER_SUPPORT = False
else:
    KAFKA_WRITER_SUPPORT = True

try:
    import redis
    import ujson
except ImportError:
    REDIS_WRITER_SUPPORT = False
else:
    import bz2
    import gzip
    import uuid
    import zlib
    REDIS_WRITER_SUPPORT = True

from functools import partial
from itertools import islice

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from sparkly.exceptions import WriteError


class SparklyWriter(object):
    """A set of tools to write DataFrames to external storages.

    Note:
        We don't expect you to be using the class directly.
        The instance of the class is available under `DataFrame` via `write_ext` attribute.
    """
    def __init__(self, df):
        self._df = df
        self._spark = df.sql_ctx.sparkSession

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
            - Redis ``redis://`` or ``rediss://``

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
            redis://redis.host/db?keyBy=id

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
        assert self._spark.has_package('datastax:spark-cassandra-connector')

        writer_options = {
            'format': 'org.apache.spark.sql.cassandra',
            'spark.cassandra.connection.host': host,
            'keyspace': keyspace,
            'table': table,
        }

        if consistency:
            writer_options['spark.cassandra.input.consistency.level'] = consistency

        if port:
            writer_options['spark.cassandra.connection.port'] = str(port)

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
        assert self._spark.has_package('org.elasticsearch:elasticsearch-spark')

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
        assert (self._spark.has_jar('mysql-connector-java') or
                self._spark.has_package('mysql:mysql-connector-java'))

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
              host,
              topic,
              key_serializer,
              value_serializer,
              port=9092,
              parallelism=None,
              options=None):
        """Writes dataframe to kafka topic.

        The schema of the dataframe should conform the pattern:

        >>>  StructType([
        ...     StructField('key', ...),
        ...     StructField('value', ...),
        ...  ])

        Parameters `key_serializer` and `value_serializer` are callables
        which get's python structure as input and should return bytes of encoded data as output.

        Args:
            host (str): Kafka host.
            topic (str): Topic to write to.
            key_serializer (function): Function to serialize key.
            value_serializer (function): Function to serialize value.
            port (int): Kafka port.
            parallelism (int|None): The max number of parallel tasks that could be executed
                during the write stage (see :ref:`controlling-the-load`).
            options (dict|None): Additional options.
        """
        assert self._spark.has_package('org.apache.spark:spark-streaming-kafka')

        if not KAFKA_WRITER_SUPPORT:
            raise NotImplementedError('kafka-python package isn\'t available. '
                                      'Use pip install sparkly[kafka] to fix it.')

        def write_partition_to_kafka(messages):
            producer = KafkaProducer(
                bootstrap_servers=['{}:{}'.format(host, port)],
                key_serializer=key_serializer,
                value_serializer=value_serializer,
            )
            for message in messages:
                as_dict = message.asDict(recursive=True)
                result = producer.send(topic, key=as_dict['key'], value=as_dict['value'])
                if result.failed():
                    raise WriteError('Error publishing to kafka: {}'.format(result.exception))

            producer.flush()
            producer.close()

            return messages

        rdd = self._df.rdd

        if parallelism:
            rdd = rdd.coalesce(parallelism)

        rdd.mapPartitions(write_partition_to_kafka).count()

    def redis(self,
              key_by,
              key_prefix=None,
              key_delimiter='.',
              group_by_key=False,
              exclude_key_columns=False,
              exclude_null_fields=False,
              expire=None,
              compression=None,
              max_pipeline_size=100,
              parallelism=None,
              mode='overwrite',
              host=None,
              port=6379,
              db=0,
              redis_client_init=None):
        """Write a dataframe to Redis as JSON.

        Args:
            key_by (list[str]): Column names that form the redis key for
                each row. The columns are concatenated in the order they
                appear.
            key_prefix (str|None): Common prefix to add to all keys from
                this DataFrame. Useful to namespace DataFrame exports.
            key_delimiter (str|.): Characters to delimit different columns
                while forming the key.
            group_by_key (bool|False): If set, group rows that share the
                same redis key together in an array before exporting. By
                default if multiple rows share the same redis key, one
                will overwrite the other.
            exclude_key_columns (bool|False): If set, exclude all columns
                that comprise the key from the value being exported to
                redis.
            exclude_null_fields (bool|False): If set, exclude all fields
                of a row that are null from the value being exported to
                redis.
            expire (int|None): Expire the keys after this number of seconds.
            compression (str|None): Compress each Redis entry using this
                protocol. Currently bzip2, gzip and zlib are supported.
            max_pipeline_size (int|100): Number of writes to pipeline.
            parallelism (int|None): The max number of parallel tasks that
                could be executed during the write stage
                (see :ref:`controlling-the-load`).
            mode (str|overwrite):
                - ``'append'``: Append to existing data on a key by key
                  basis.
                - ``'ignore'``: Silently ignore if data already exists.
                - ``'overwrite'``: Flush all existing data before writing.
            host (str|None): Redis host. Either this or redis_client_init
                must be provided. See below.
            port (int|6379): Port redis is listening to.
            db (int|0): Redis db to write to.
            redis_client_init (callable|None): Bypass internal redis
                client initialization by passing a function that does it,
                no arguments required. For example this could be
                ``redis.StrictRedis.from_url`` with the appropriate url and
                ``kwargs`` already set through ``functools.partial``.
                This option overrides other conflicting arguments.

        Raises:
            NotImplementedError: if `redis-py` is not installed.
            AssertionError: if nor host neither ``redis_client_init`` are
                provided.
            ValueError: if any of the ``expire``, ``compression``,
                ``max_pipeline_size`` or ``mode`` options assume an
                invalid value.
        """
        if not REDIS_WRITER_SUPPORT:
            raise NotImplementedError(
                'redis package is not available. Use pip install sparkly[redis] to fix it.'
            )

        assert host or redis_client_init, \
            'redis: At least one of host or redis_client_init must be provided.'

        if expire is not None and expire < 1:
            raise ValueError('redis: expire must be positive')

        if compression not in {None, 'bzip2', 'gzip', 'zlib'}:
            raise ValueError(
                'redis: bzip2, gzip and zlib are the only supported compression codecs.'
            )

        if max_pipeline_size < 1:
            raise ValueError('redis: max pipeline size must be positive')

        if mode not in {'append', 'ignore', 'overwrite'}:
            raise ValueError(
                'redis: only append (default), ignore and overwrite modes are supported.'
            )

        key_name = '_sparkly_redis_key_col_{}'.format(uuid.uuid4().hex)
        value_name = '_sparkly_redis_value_col_{}'.format(uuid.uuid4().hex)

        # Compute the key
        df = self._df.withColumn(
            key_name,
            F.concat_ws(
                key_delimiter,
                *(
                    ([F.lit(key_prefix).astype(T.StringType())] if key_prefix else []) +
                    [F.col(col_name).astype(T.StringType()) for col_name in key_by]
                )
            )
        )

        if exclude_key_columns:
            for col_name in key_by:
                df = df.drop(col_name)

        if group_by_key:
            df = (
                df
                .withColumn(
                    value_name,
                    F.struct(*[col for col in df.columns if col != key_name])
                )
                .groupBy(key_name)
                .agg(F.collect_list(value_name).alias(value_name))
            )

        # Repartition if needed to achieve specified parallelism
        df = df.coalesce(parallelism or df.rdd.getNumPartitions())

        def compress(data, protocol=None):
            if protocol is None:
                return data
            elif protocol == 'bzip2':
                return bz2.compress(data)
            elif protocol == 'zlib':
                return zlib.compress(data)
            elif protocol == 'gzip':
                try:
                    return gzip.compress(data)
                except AttributeError:  # py27
                    compressor = zlib.compressobj(
                        zlib.Z_DEFAULT_COMPRESSION,
                        zlib.DEFLATED,
                        zlib.MAX_WBITS | 16,
                    )
                    return compressor.compress(data) + compressor.flush()
            else:
                raise ValueError('unknown compression protocol {}'.format(protocol))

        # Define the function that will write each partition to redis
        def _write_redis(partition, redis, expire, compress, max_pipeline_size, mode):
            pipeline = redis().pipeline(transaction=False)
            add_to_pipeline = partial(pipeline.set, ex=expire, nx=(mode == 'ignore'))

            partition = iter(partition)
            while True:
                rows = islice(partition, max_pipeline_size)
                for row in rows:
                    row = row.asDict(recursive=True)

                    key = row.pop(key_name)
                    if group_by_key:
                        row = row[value_name]
                    if exclude_null_fields:
                        for null_field in list(f for f in row.keys() if row[f] is None):
                            del row[null_field]
                    data = bytes(ujson.dumps(row).encode('ascii'))
                    if compress:
                        data = compress(data)

                    add_to_pipeline(key, data)

                if not len(pipeline):
                    break

                pipeline.execute()

        redis_client_init = redis_client_init or partial(redis.StrictRedis, host, port, db=db)

        if mode == 'overwrite':
            redis_client_init().flushdb()

        df.foreachPartition(
            partial(
                _write_redis,
                redis=redis_client_init,
                expire=expire,
                compress=partial(compress, protocol=compression),
                max_pipeline_size=max_pipeline_size,
                mode=mode,
            )
        )

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
        parallelism = parsed_qs.pop('parallelism', None)
        if parallelism:
            df = self._df.coalesce(int(parallelism))
        else:
            df = self._df

        if 'partitionBy' in parsed_qs:
            parsed_qs['partitionBy'] = parsed_qs.pop('partitionBy').split(',')

        df.write.save(
            path=parsed_url.path,
            format=parsed_url.scheme,
            **parsed_qs
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

    def _resolve_redis(self, parsed_url, parsed_qs):
        # Extract all the custom options
        try:
            key_by = parsed_qs.pop('keyBy')
        except KeyError:
            raise AssertionError('redis: url must define keyBy columns to construct redis key')
        key_by = key_by.split(',')

        key_prefix = parsed_qs.pop('keyPrefix', None)
        key_delimiter = parsed_qs.pop('keyDelimiter', '.')

        def _parse_boolean(parameter, default='false'):
            value = parsed_qs.pop(parameter, default).lower()
            if value not in {'true', 'false'}:
                raise ValueError(
                    'redis: true and false (default) are the only supported {} values'
                    .format(parameter)
                )
            return value == 'true'

        group_by_key = _parse_boolean('groupByKey')
        exclude_key_columns = _parse_boolean('excludeKeyColumns')
        exclude_null_fields = _parse_boolean('excludeNullFields')

        try:
            expire = int(parsed_qs.pop('expire'))
        except KeyError:
            expire = None
        except (TypeError, ValueError):
            raise ValueError('redis: expire must be a base 10, positive integer')

        compression = parsed_qs.pop('compression', None)

        try:
            max_pipeline_size = int(parsed_qs.pop('maxPipelineSize', 100))
        except (TypeError, ValueError):
            raise ValueError('redis: maxPipelineSize must be a base 10, positive integer')

        parallelism = parsed_qs.pop('parallelism', None)
        mode = parsed_qs.pop('mode', 'append')

        # Reconstruct whatever remains of the original URL
        url = parsed_url._replace(query='&'.join('='.join(o) for o in parsed_qs)).geturl()

        return self.redis(
            key_by=key_by,
            key_prefix=key_prefix,
            key_delimiter=key_delimiter,
            group_by_key=group_by_key,
            exclude_key_columns=exclude_key_columns,
            exclude_null_fields=exclude_null_fields,
            expire=expire,
            compression=compression,
            max_pipeline_size=max_pipeline_size,
            parallelism=parallelism,
            mode=mode,
            # and then let redis-py decode it
            redis_client_init=partial(redis.StrictRedis.from_url, url)
        )

    def _resolve_rediss(self, parsed_url, parsed_qs):
        return self._resolve_redis(parsed_url, parsed_qs)


def attach_writer_to_dataframe():
    """A tiny amount of magic to attach write extensions."""
    def write_ext(self):
        return SparklyWriter(self)

    DataFrame.write_ext = property(write_ext)
