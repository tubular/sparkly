from sparkle.utils import context_has_package, config_reader_writer


def cassandra(df, host, keyspace, table, consistency='QUORUM', mode='append', options=None):
    """Write dataframe into the cassandra table.

    Args:
        df (pyspark.sql.DataFrame)
        host (str)
        keyspace (str)
        table (str)
        consistency (str)
        mode (str): Spark save mode,
            http://spark.apache.org/docs/latest/sql-programming-guide.html#save-modes
        options (dict[str, str]): Additional options to `org.apache.spark.sql.cassandra` format.
    """
    assert context_has_package(df.sql_ctx, 'datastax:spark-cassandra-connector')

    writer = config_reader_writer(df.write.format('org.apache.spark.sql.cassandra'), {
        'spark_cassandra_connection_host': host,
        'spark_cassandra_output_consistency_level': consistency,
        'keyspace': keyspace,
        'table': table,
    })

    config_reader_writer(writer, options).mode(mode).save()


def csv(df, path, header=False, mode='error', options=None):
    """Write dataframe into csv file.

    Args:
        df (pyspark.sql.DataFrame)
        path (str)
        mode (str): Spark save mode,
            http://spark.apache.org/docs/latest/sql-programming-guide.html#save-modes
        header (bool): First row is a header.
    """
    assert context_has_package(df.sql_ctx, 'com.databricks:spark-csv')

    writer = config_reader_writer(df.write.format('com.databricks.spark.csv'), {
        'header': str(header).lower(),
    })

    return config_reader_writer(writer, options).mode(mode).save(path)


def elastic(df, host, es_index, es_type, mode='append', options=None):
    """Write dataframe into the ES index.

    Args:
        df (pyspark.sql.DataFrame)
        host (str)
        index_type (str): E.g. 'intelligence/video'.
        mode (str): Spark save mode,
            http://spark.apache.org/docs/latest/sql-programming-guide.html#save-modes
        options (dict[str, str]): Additional options to `org.elasticsearch.spark.sql` format.
    """
    assert context_has_package(df.sql_ctx, 'org.elasticsearch:elasticsearch-spark')

    writer = config_reader_writer(df.write.format('org.elasticsearch.spark.sql'), {
        'es.nodes': host,
    })

    return config_reader_writer(writer, options).mode(mode).save('{}/{}'.format(es_index, es_type))
