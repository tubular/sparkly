import logging
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType


logger = logging.getLogger(__name__)


def get_all_tables(hc):
    """Returns all tables available in metastore.

    Args:
        hc (HiveContext)

    Returns:
        (list)
    """
    return [o.tableName for o in hc.sql("SHOW TABLES").collect()]


def table_exists(hc, table_name):
    """Returns if table exists in metastore or not.

     Args:
        hc (HiveContext)

    Returns:
        (bool)
    """
    return table_name in get_all_tables(hc)


def create_table(hc, table_name, df, location, partition_by=None, table_format=None):
    """Creates table by DataFrame.

    Args:
        hc (pyspark.sql.context.HiveContext)
        table_name (str)
        df (pyspark.sql.dataframe.DataFrame)
        partition_by (list)
        output_path (str)

    Returns:
        None
    """
    create_table_sql = get_create_table_statement(
        table_name,
        df,
        partition_by=partition_by,
        location=location,
        table_format=table_format,
    )

    hc.sql(create_table_sql)

    if partition_by:
        hc.sql('MSCK REPAIR TABLE {}'.format(table_name))


def set_table_property(hc, table, property, value):
    """Sets table property in Hive Metastore.

    Args:
        hc (HiveContext)
        table (str): table name
        property (str): property name
        value (str): value of property
    """
    hc.sql("ALTER TABLE {} SET TBLPROPERTIES ('{}'='{}')".format(
        table, property, value
    ))


def get_table_property(hc, table, property, to_type=None):
    """Gets value of table property.

    Args:
        hc (HiveContext)
        table (str)
        property (str)
        to_type (type): type to coarse to

    Returns:
        (any)
    """
    if not to_type:
        to_type = str

    df = hc.sql("SHOW TBLPROPERTIES {}('{}')".format(table, property))
    prop_val = df.collect()[0].result.strip()

    if 'does not have property' not in prop_val:
        return to_type(prop_val)


def get_create_table_statement(table_name, schema, location, partition_by=None, table_format=None):
    """Converts pyspark schema to hive CREATE TABLE definition.

    Args:
        table_name (str): name of a table.
        schema (dict|pyspark.sql.dataframe.DataFrame|pyspark.sql.types.StructType): \
            source of schema. Dict should be in format of result of method DataFrame.jsonValue()
        location (str): table data path, s3 bucket path (or hdfs if you like).
        partition_by (list|None): list of partitioning fields.
        format (str): format of tables data files.

    Returns
        (str) create table statement.
    """
    if isinstance(schema, DataFrame):
        schema = schema.schema.jsonValue()
    elif isinstance(schema, StructType):
        schema = schema.jsonValue()

    if not table_format:
        table_format = 'PARQUET'

    if not partition_by:
        partition_by = []

    columns = []
    partitions_map = {}
    for field in schema['fields']:
        logger.debug('Analyzing :: {} :: '.format(field['name']))
        if field['name'] in partition_by:
            partitions_map[field['name']] = '`{}` {}'.format(field['name'], _type_to_hql(field))
        else:
            columns.append(
                '`{}` {}'.format(field['name'], _type_to_hql(field))
            )

    if not partition_by:
        return 'CREATE EXTERNAL TABLE `{}` ({}) ' \
               'STORED AS {} ' \
               'LOCATION \'{}\''.format(table_name,
                                        ', '.join(columns),
                                        table_format,
                                        location)
    else:
        return 'CREATE EXTERNAL TABLE `{}` ({}) ' \
               'PARTITIONED BY ({}) ' \
               'STORED AS {} ' \
               'LOCATION \'{}\''.format(table_name,
                                        ', '.join(columns),
                                        ', '.join(partitions_map[item] for item in partition_by),
                                        table_format,
                                        location)


# simple df types map to hive types
_type_map = {
    'string': 'string',
    'float': 'float',
    'double': 'double',
    'long': 'bigint',
    'integer': 'int',
    'timestamp': 'timestamp',
    'boolean': 'boolean',
}


def _type_to_hql(schema, level_=0):
    """Converts dataframe type definition to hive type definition.

    Args:
        schema (dict) pyspark type definition.
        level_ (int) level of nesting, debug only,
                     no need to specify this parameter explicitly.

    Returns:
        (str) hive type definition.
    """
    if isinstance(schema, str):
        logger.debug('{} {}'.format(':' * level_, schema))
        if schema not in _type_map:
            raise NotImplementedError('{} is not supported in this place'.format(schema))
        else:
            return _type_map[schema]

    type_ = schema['type']

    if type_ == 'struct':
        logger.debug('{} STRUCT'.format(':' * level_))
        definitions = []
        for field in schema['fields']:
            logger.debug('{} STRUCT FIELD {}'.format(':' * level_, field['name']))
            definitions.append('`{}`:{}'.format(
                field['name'],
                _type_to_hql(field, level_=level_ + 2)
            ))

        return 'struct<{}>'.format(','.join(definitions))

    elif type_ == 'array':
        logger.debug('{} ARRAY'.format(':' * level_))
        return 'array<{}>'.format(_type_to_hql(schema['elementType'], level_=level_ + 2))

    elif type_ == 'map':
        logger.debug('{} MAP'.format(':' * level_))
        return 'map<{},{}>'.format(_type_to_hql(schema['keyType'], level_=level_ + 2),
                                   _type_to_hql(schema['valueType'], level_=level_ + 2))

    elif isinstance(type_, dict):
        return _type_to_hql(type_)

    else:
        logger.debug('{} RECURSE'.format(':' * level_))
        return _type_to_hql(type_, level_=level_ + 2)


if __name__ == '__main__':
    from sparkle import SparkleContext

    logging.basicConfig(level=logging.DEBUG)

    class Cnx(SparkleContext):
        packages = ['datastax:spark-cassandra-connector:1.5.0-M3-s_2.10',
                    'org.elasticsearch:elasticsearch-spark_2.10:2.3.0',
                    'org.apache.spark:spark-streaming-kafka_2.10:1.6.1',
                    ]

    sql = Cnx()
