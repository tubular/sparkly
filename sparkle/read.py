from sparkle.utils import context_has_package, config_reader_writer


def cassandra(hc, host, keyspace, table, consistency='QUORUM', options=None):
    """Create dataframe from the cassandra table.

    Args:
        hc (sparkle.SparkleContext)
        host (str)
        keyspace (str)
        table (str)
        consistency (str)
        options (dict[str,str]): Additional options for `org.apache.spark.sql.cassandra` format.

    Returns:
        pyspark.sql.DataFrame
    """
    assert context_has_package(hc, 'datastax:spark-cassandra-connector')

    reader = config_reader_writer(hc.read.format('org.apache.spark.sql.cassandra'), {
        'spark_cassandra_connection_host': host,
        'spark_cassandra_input_consistency_level': consistency,
        'keyspace': keyspace,
        'table': table,
    })

    return config_reader_writer(reader, options).load()


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
        'inferSchema': str(not custom_schema).lower(),
    })

    if custom_schema:
        reader = reader.schema(custom_schema)

    return config_reader_writer(reader, options).load(path)


def elastic(hc, host, es_index, es_type, query='', fields=None, options=None):
    """Create dataframe from the elasticsearch index.

    Args:
        hc (sparkle.SparkleContext)
        host (str)
        es_index (str)
        es_type (str)
        query (str): pre-filter es documents, e.g. '?q=views:>10'.
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

    return config_reader_writer(reader, options).load('{}/{}'.format(es_index, es_type))
