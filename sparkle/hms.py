"""Utilities for more convenient usage of Hive related utils.

Usage:

    >>> sparkle.hms.create_table(Schema(...), 's3://path')
    >>> sparkle.hms.replace(Schema(...), 's3://path', partition_by=['date'])
    >>> sparkle.hms.table(hc, 'my_table').exists()
    >>> sparkle.hms.table(hc, 'your_table').get_all_properties() == {'propertyA': 'valueA', ...}
    >>> sparkle.hms.table(hc, 'my_table').get_property('help') == 'Hello'
    >>> sparkle.hms.table(hc, 'my_table').set_property('help', 'Hey ho')
"""

import logging
import re

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

from sparkle.utils import absolute_path


logger = logging.getLogger(__name__)


class table(object):
    """Represents a table in HiveMetastore.

    Provides meta data operations on a table.

    >>> table(hc, 'my_table').exists()
    >>> table(hc, 'my_table').set_property('prop', 'val')
    """

    def __init__(self, hc, table_name=None):
        if isinstance(hc, str):
            raise ValueError('Looks like you specified table name instead '
                             'of passing HiveContext as a first parameter')

        self.hc = hc
        self.table_name = table_name

    def exists(self):
        """Checks if table exists.

        Returns:
            bool
        """
        return self.table_name in get_all_tables(self.hc)

    def set_property(self, name, value):
        """Sets table property.

        Args:
            name (str): name of the property.
            value (str): value of the proporty.

        Returns:
            table_manager: self.
        """
        self.hc.sql("ALTER TABLE {} SET TBLPROPERTIES ('{}'='{}')".format(
            self.table_name, name, value
        ))
        return self

    def get_property(self, name, to_type=None):
        """Gets table property.

        Args:
            name (str): name of the property.
            to_type (type): type to coarce to, str by default.

        Returns:
            any
        """
        if not to_type:
            to_type = str

        df = self.hc.sql("SHOW TBLPROPERTIES {}('{}')".format(self.table_name, name))
        prop_val = df.collect()[0].result.strip()

        if 'does not have property' not in prop_val:
            return to_type(prop_val)

    def get_all_properties(self):
        """Returns all table properties.

        Returns:
            dict: property names to values.
        """
        res = self.hc.sql("""
            SHOW TBLPROPERTIES {}
        """.format(self.table_name)).collect()

        return dict([item.result.split() for item in res])

    def df(self):
        """Returns dataframe for the managed table.

        Returns:
            pyspark.sql.dataframe.DataFrame
        """
        return self.hc.table(self.table_name)


def get_all_tables(hc):
    """Returns all tables available in metastore.

    Args:
        hc (HiveContext)

    Returns:
        (list)
    """
    return [o.tableName for o in hc.sql("SHOW TABLES").collect()]


def create_table(hc, table_name, df, location,
                 partition_by=None,
                 table_format=None,
                 properties=None):
    """Creates table by DataFrame.

    Args:
        hc (pyspark.sql.context.HiveContext)
        table_name (str): name of new table.
        schema (pyspark.sql.dataframe.DataFrame): schema.
        location (str): location of data.
        partition_by (list): partitioning columns.
        table_format (str): default is parquet.
        properties (dict): properties to assign to the table.
    """
    create_table_sql = _get_create_table_statement(
        table_name,
        df,
        partition_by=partition_by,
        location=location,
        table_format=table_format,
    )

    hc.sql(create_table_sql)

    if properties:
        table_manager = table(hc, table_name)
        for key, val in properties.items():
            table_manager.set_property(key, val)

    if partition_by:
        hc.sql('MSCK REPAIR TABLE {}'.format(table_name))

    return table(hc, table_name)


def replace_table(hc, table_name, schema, location, partition_by=None):
    """Replaces table `table_name` with data represented by schema, location.

    Args:
        hc (pyspark.sql.context.HiveContext)
        table_name (str): table name.
        schema (pyspark.sql.dataframe.DataFrame): schema.
        location (str): data location, ex.: s3://path/tp/data.
        partition_by (list): fields the data partitioned by.
    """
    old_table = '{}_OLD'.format(table_name)
    temp_table = '{}_NEW'.format(table_name)

    table_manager = table(hc, table_name)
    old_table_props = table_manager.get_all_properties()

    create_table(
        hc,
        temp_table,
        schema,
        location=location,
        partition_by=partition_by,
        properties=old_table_props,
    )

    hc.sql("""
      ALTER TABLE {} RENAME TO {}
    """.format(table_name, old_table))

    hc.sql("""
      ALTER TABLE {} RENAME TO {}
    """.format(temp_table, table_name))

    hc.sql("""
      DROP TABLE {}
    """.format(old_table))

    return table(hc, old_table)


def _get_create_table_statement(table_name, schema, location, partition_by=None, table_format=None):
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
    'date': 'date',
}


_type_map_re = [
    # input regexp, output template
    (re.compile(r'decimal\((?P<precision>[0-9]+),(?P<scale>[0-9]+)\)', re.IGNORECASE),
     'decimal({precision},{scale})'),
]


def _type_to_hql(schema, level_=0):
    """Converts dataframe type definition to hive type definition.

    Args:
        schema (dict): Pyspark type definition.
        level_ (int): Level of nesting, debug only,
            no need to specify this parameter explicitly.

    Returns:
        (str) hive type definition.
    """
    if isinstance(schema, str):
        logger.debug('{} {}'.format(':' * level_, schema))

        if schema in _type_map:
            return _type_map[schema]

        for regex, template in _type_map_re:
            match = regex.match(schema)
            if match:
                return template.format(**match.groupdict())

        raise NotImplementedError('{} is not supported in this place'.format(schema))

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
        jars = [
            absolute_path(__file__, '..', 'tests',
                          'integration', 'resources',
                          'mysql-connector-java-5.1.39-bin.jar'),
        ]

    sql = Cnx()
