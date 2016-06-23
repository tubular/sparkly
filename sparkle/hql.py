from pyspark.sql import DataFrame
from pyspark.sql.types import StructType


def get_create_table_statement(table_name, schema, location, partition_by=None, format='PARQUET'):
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

    if not partition_by:
        partition_by = []

    columns = []
    partitioning = []
    for field in schema['fields']:
        if field['name'] in partition_by:
            partitioning.append(
                '`{}` {}'.format(field['name'], _type_to_hql(field))
            )
        else:
            columns.append(
                '`{}` {}'.format(field['name'], _type_to_hql(field))
            )

    if not partitioning:
        return 'CREATE EXTERNAL TABLE `{}` ({}) ' \
               'STORED AS {} ' \
               'LOCATION \'{}\''.format(table_name,
                                        ', '.join(columns),
                                        format,
                                        location)
    else:
        return 'CREATE EXTERNAL TABLE `{}` ({}) ' \
               'PARTITIONED BY ({}) ' \
               'STORED AS {} ' \
               'LOCATION \'{}\''.format(table_name,
                                        ', '.join(columns),
                                        ', '.join(partitioning),
                                        format,
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


def _type_to_hql(schema):
    """Converts dataframe type definition to hive type definition.

    Args:
        schema (dict)

    Returns:
        (str)
    """
    if isinstance(schema, str):
        if schema not in _type_map:
            raise NotImplementedError
        else:
            return _type_map[schema]

    type_ = schema['type']

    if type_ == 'struct':
        definitions = []
        for field in schema['fields']:
            definitions.append('`{}`:{}'.format(field['name'], _type_to_hql(field)))

        return 'struct<{}>'.format(','.join(definitions))

    elif isinstance(type_, dict):
        if type_['type'] == 'map':
            return 'map<{},{}>'.format(_type_to_hql(type_['keyType']),
                                       _type_to_hql(type_['valueType']))
        elif type_['type'] == 'array':
            return 'array<{}>'.format(_type_to_hql(type_['elementType']))
        else:
            return _type_to_hql(type_)

    else:
        return _type_to_hql(type_)
