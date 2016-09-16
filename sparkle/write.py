import copy

try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse

from sparkle.utils import (context_has_package, config_reader_writer,
                           context_has_jar, to_parsed_url_and_options)


def fs(df, path, partition_by=None, mode=None, output_format=None, options=None):
    """Writes dataframe to a file system.

    Args:
        df (pyspark.sql.DataFrame):
        path (str): S3 or local fs path.
        partition_by (list): Fields to partition by.
        output_format (str|None): Output files format.
    """
    writer = df.write\
        .mode(mode)\
        .partitionBy(*(partition_by or []))\
        .format(output_format)
    config_reader_writer(writer, options).save(path)


def cassandra(df, host, keyspace, table, consistency=None, mode=None, options=None):
    """Write dataframe into the cassandra table.

    Args:
        df (pyspark.sql.DataFrame)
        host (str)
        keyspace (str)
        table (str)
        consistency (str|None): Write consitency level: ONE, QUORUM, ALL, etc.
        mode (str|None): Spark save mode,
            http://spark.apache.org/docs/latest/sql-programming-guide.html#save-modes
        options (dict[str, str]): Additional options to `org.apache.spark.sql.cassandra` format.
    """
    assert context_has_package(df.sql_ctx, 'datastax:spark-cassandra-connector')

    options = options or {}
    options = copy.deepcopy(options)
    options['spark_cassandra_connection_host'] = host
    options['keyspace'] = keyspace
    options['table'] = table

    if consistency:
        options['spark_cassandra_output_consistency_level'] = consistency

    config_reader_writer(
        df.write.format('org.apache.spark.sql.cassandra'), options
    ).mode(mode).save()


def csv(df, path, header=False, mode=None, options=None):
    """Write dataframe into csv file.

    Args:
        df (pyspark.sql.DataFrame)
        path (str)
        mode (str|None): Spark save mode,
            http://spark.apache.org/docs/latest/sql-programming-guide.html#save-modes
        header (bool): First row is a header.
    """
    assert context_has_package(df.sql_ctx, 'com.databricks:spark-csv')

    writer = config_reader_writer(df.write.format('com.databricks.spark.csv'), {
        'header': str(header).lower(),
    })

    config_reader_writer(writer, options).mode(mode).save(path)


def elastic(df, host, es_index, es_type, mode=None, options=None):
    """Write dataframe into the ES index.

    Args:
        df (pyspark.sql.DataFrame)
        host (str)
        index_type (str): E.g. 'intelligence/video'.
        mode (str|None): Spark save mode,
            http://spark.apache.org/docs/latest/sql-programming-guide.html#save-modes
        options (dict[str, str]): Additional options to `org.elasticsearch.spark.sql` format.
    """
    assert context_has_package(df.sql_ctx, 'org.elasticsearch:elasticsearch-spark')

    writer = config_reader_writer(df.write.format('org.elasticsearch.spark.sql'), {
        'es.nodes': host,
    })

    config_reader_writer(writer, options).\
        mode(mode).\
        save('{}/{}'.format(es_index, es_type))


def mysql(df, host, database, table, mode=None, options=None):
    """Writes dataframe into mysql table.

    Args:
        df (pyspark.sql.DataFrame)
        host (str)
        database (str)
        table (str): Mysql table.
        mode (str|None): Write mode.
        options (dict): Additional options.
    """
    assert context_has_jar(df.sql_ctx, 'mysql-connector-java')

    options = options or {}
    options = copy.deepcopy(options)
    options['driver'] = 'com.mysql.jdbc.Driver'

    df.write.jdbc(
        'jdbc:mysql://{}:3306/{}'.format(host, database),
        table,
        mode=mode,
        properties=options,
    )


def by_url(df, url):
    """Writes DataFrame to destination specified by `url`.

    This method is an attempt to unify all write destinations.

    Supported formats:
        - CSV (csv://)
        - Parquet (parquet://)
        - Elastic (elastic://)
        - Cassandra (cassandra://)
        - Mysql (mysql://)

    Examples:
        - 'csv:s3://some-s3-bucket/some-s3-key?partition_by=date,platform'
        - 'cassandra://tital-ii/natural/youtube_temp?consistency=ONE&mode=append'
        - 'parquet:///var/log/?partition_by=date'
        - 'elastic://elastic.host/es_index/es_type'
        - 'mysql://mysql.host/database/table'

    Args:
        df (pyspark.sql.DataFrame): data to be written.
        url (str): url of destination.
    """
    _by_url_registry = {
        'parquet': _fs_resolver,
        'csv': _fs_resolver,
        'cassandra': _cassandra_resolver,
        'mysql': _mysql_resolver,
        'elastic': _elastic_resolver,
    }
    scheme = urlparse(url).scheme
    try:
        _by_url_registry[scheme](df, url)
    except KeyError:
        raise NotImplementedError('Destination specified in url is not supported: {}'.format(url))


def _fs_resolver(df, url):
    inp, options = to_parsed_url_and_options(url)
    partition_by = options.pop('partition_by', None)
    output_format = inp.scheme
    mode = options.pop('mode', None)
    fs(df,
       inp.path,
       partition_by=partition_by.split(',') if partition_by else None,
       output_format=output_format,
       mode=mode,
       options=options,
       )


def _cassandra_resolver(df, url):
    inp, options = to_parsed_url_and_options(url)
    _, db, table = inp.path.split('/')
    mode = options.pop('mode', None)
    consistency = options.pop('consistency', None)
    cassandra(
        df, inp.netloc,
        keyspace=db,
        table=table,
        mode=mode,
        consistency=consistency,
        options=options,
    )


def _mysql_resolver(df, url):
    inp, options = to_parsed_url_and_options(url)
    _, db, table = inp.path.split('/')
    mode = options.pop('mode', None)
    mysql(
        df, inp.netloc,
        database=db,
        table=table,
        mode=mode,
        options=options,
    )


def _elastic_resolver(df, url):
    inp, options = to_parsed_url_and_options(url)
    _, index, type_ = inp.path.split('/')
    mode = options.pop('mode', None)
    elastic(
        df, inp.netloc,
        es_index=index,
        es_type=type_,
        mode=mode,
        options=options,
    )
