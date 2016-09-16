import copy
import ujson as json
from pyspark.streaming.kafka import KafkaUtils, OffsetRange

from sparkle.schema_parser import generate_structure_type, parse_schema

try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse

from sparkle.utils import (context_has_package, config_reader_writer,
                           context_has_jar, to_parsed_url_and_options)


def cassandra(hc, host, keyspace, table, consistency=None, parallelism=None, options=None):
    """Create dataframe from the cassandra table.

    Args:
        hc (sparkle.SparkleContext)
        host (str)
        keyspace (str)
        table (str)
        consistency (str): Read consitency level: ONE, QUORUM, ALL, etc.
        parallelism (str): Desired level of parallelism.
        options (dict[str,str]): Additional options for `org.apache.spark.sql.cassandra` format.

    Returns:
        pyspark.sql.DataFrame
    """
    assert context_has_package(hc, 'datastax:spark-cassandra-connector')

    options = options or {}
    options = copy.deepcopy(options)

    options['spark_cassandra_connection_host'] = host
    options['keyspace'] = keyspace
    options['table'] = table

    if consistency:
        options['spark_cassandra_input_consistency_level'] = consistency

    reader = config_reader_writer(
        hc.read.format('org.apache.spark.sql.cassandra'), options
    ).load()

    if parallelism:
        reader = reader.coalesce(parallelism)

    return reader


def csv(hc, path, custom_schema=None, header=True, options=None):
    """Create dataframe from the csv file.

    Args:
        hc (sparkle.SparkleContext)
        path (str): Path to file.
        custom_schema (pyspark.sql.types.DataType): Force custom schema.
        header (bool): First row is a header.
        options (dict[str,str]): Additional options for `com.databricks.spark.csv` format.

    Returns:
        pyspark.sql.DataFrame
    """
    assert context_has_package(hc, 'com.databricks:spark-csv')

    reader = config_reader_writer(hc.read.format('com.databricks.spark.csv'), {
        'header': str(header).lower(),
        'inferSchema': 'false' if custom_schema else 'true',
    })

    if custom_schema:
        reader = reader.schema(custom_schema)

    return config_reader_writer(reader, options).load(path)


def elastic(hc, host, es_index, es_type, query='', fields=None, parallelism=None, options=None):
    """Create dataframe from the elasticsearch index.

    Args:
        hc (sparkle.SparkleContext)
        host (str)
        es_index (str)
        es_type (str)
        query (str): Pre-filter es documents, e.g. '?q=views:>10'.
        fields (list[str]): Select only specified fields.
        options (dict[str,str]): Additional options for `org.elasticsearch.spark.sql` format.

    Returns:
        pyspark.sql.DataFrame
    """
    assert context_has_package(hc, 'org.elasticsearch:elasticsearch-spark')

    reader = config_reader_writer(hc.read.format('org.elasticsearch.spark.sql'), {
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


def mysql(hc, host, database, table, options=None):
    """Create dataframe from the mysql table.

    Should be usable for rds, aurora, etc.
    Options should at least contain user and password.

    Args:
        hc (sparkle.SparkleContext): Hive context.
        host (str): Server address.
        database (str): Database to connect to.
        table (str): Table to read rows from.
        options (dict[str,str]): Additional options for `org.elasticsearch.spark.sql` format.

    Returns:
        pyspark.sql.DataFrame
    """
    assert context_has_jar(hc, 'mysql-connector-java')

    reader = config_reader_writer(hc.read.format('jdbc'), {
        'url': 'jdbc:mysql://{}:3306/{}'.format(host, database),
        'driver': 'com.mysql.jdbc.Driver',
        'dbtable': table,
    })
    return config_reader_writer(reader, options).load()


def kafka(hc, brokers, offset_ranges):
    """Creates dataframe from specified set of messages from Kafka topic.

    Args:
        hc (HiveContext):
        brokers (list): Additional kafka parameters, see KafkaUtils.createRDD docs.
        offset_ranges (list[(str, int, int, int)]): List of partition ranges
            [(topic, partition, start_offset, end_offset)].

    Returns:
        pyspark.rdd.RDD
    """
    assert context_has_package(hc, 'org.apache.spark:spark-streaming-kafka')

    def _default_decoder(item):
        return json.loads(item.decode('utf-8'))

    kafka_params = {
        'metadata.broker.list': ','.join(brokers)
    }
    offset_ranges = [OffsetRange(topic, partition, start_offset, end_offset)
                     for topic, partition, start_offset, end_offset in offset_ranges]
    rdd = KafkaUtils.createRDD(hc._sc, kafka_params, offset_ranges,
                               valueDecoder=_default_decoder)

    return rdd


def by_url(hc, url):
    """Create dataframe from data specified by URL.

    The main idea behind this method is to unify data access interface for different
    formats and locations. A generic schema looks like:
        format:[protocol:]//host[/location][?configuration]

    Supported formats:
        - Hive Metastore Table (table://)
        - Parquet (parquet://)
        - CSV (csv://)
        - Elastic (elastic://)
        - Cassandra (cassandra://)
        - MySQL (mysql://)
        - Kafka (kafka://)

    Examples:
        - `table://table_name`
        - `csv:s3://some-bucket/some_directory?header=true`
        - `csv://path/on/local/file/system?header=false`
        - `parquet:s3://some-bucket/some_directory`
        - `elastic://elasticsearch.host/es_index/es_type?parallelism=8`
        - `cassandra://cassandra.host/keyspace/table?consistency=QUORUM`
        - `mysql://mysql.host/database/table`
        - `kafka://kafka.host/topic`

    TODO (drudim):
        - I don't like how kafka url looks like.

    Args:
        hc (sparkle.SparkleContext): Spark Context.
        url (str): describes data source.

    Returns:
        pyspark.sql.DataFrame
    """
    _by_url_registry = {
        'parquet': _fs_resolver,
        'csv': _csv_resolver,
        'cassandra': _cassandra_resolver,
        'mysql': _mysql_resolver,
        'elastic': _elastic_resolver,
        'kafka': _kafka_resolver,
        'table': _table_resolver,
    }

    scheme = urlparse(url).scheme
    try:
        return _by_url_registry[scheme](hc, url)
    except KeyError:
        raise NotImplementedError('Data source is not supported: {}'.format(url))


def _cassandra_resolver(hc, url):
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

    return cassandra(hc, inp.netloc, keyspace, table, **kwargs)


def _elastic_resolver(hc, url):
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
    return elastic(hc, host, es_index, es_type,
                   query=query, fields=fields,
                   parallelism=parallelism, options=options)


def _table_resolver(hc, url):
    inp, options = to_parsed_url_and_options(url)
    return hc.table(inp.netloc)


def _fs_resolver(hc, url):
    inp, options = to_parsed_url_and_options(url)
    return hc.read.format(inp.scheme).options(**options).load(inp.path)


def _mysql_resolver(hc, url):
    inp, options = to_parsed_url_and_options(url)
    db, table = inp.path[1:].split('/', 1)
    return mysql(hc, inp.netloc, db, table, options=options)


def _kafka_resolver(hc, url):
    inp, options = to_parsed_url_and_options(url)
    offset_ranges = []
    for item in inp.path[1:].split('/'):
        if not item:
            continue

        topic, partition, start_offset, end_offset = item.split(',')
        offset_ranges.append(
            (topic, int(partition), int(start_offset), int(end_offset))
        )

    return kafka(hc, inp.netloc.split(','), offset_ranges)


def _csv_resolver(hc, url):
    inp, options = to_parsed_url_and_options(url)
    kwargs = {}

    header = options.pop('header', None)
    if header is not None:
        kwargs['header'] = header == 'true'

    custom_schema = options.pop('custom_schema', None)
    if custom_schema:
        custom_schema = generate_structure_type(parse_schema(custom_schema))
        kwargs['custom_schema'] = custom_schema

    if options:
        kwargs['options'] = options

    return csv(hc, inp.path, **kwargs)
