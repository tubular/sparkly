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

import logging
import re

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType


logger = logging.getLogger(__name__)


class SparklyHiveMetastoreManager(object):
    """A set of tools to interact with HiveMetastore."""

    def __init__(self, hc):
        self.hc = hc

    def table(self, table_name):
        return Table(self, table_name)

    def get_all_tables(self):
        """Returns all tables available in metastore.

        Returns:
            list
        """
        return [o.tableName for o in self.hc.sql("SHOW TABLES").collect()]

    def create_table(self,
                     table_name,
                     schema,
                     location,
                     partition_by=None,
                     table_format=None,
                     properties=None):
        """Creates table in Hive Metastore.

        Args:
            table_name (str): name of new Table.
            schema (pyspark.sql.dataframe.DataFrame): schema.
            location (str): location of data.
            partition_by (list): partitioning columns.
            table_format (str): default is parquet.
            properties (dict): properties to assign to the table.

        Returns:
            Table
        """
        create_table_sql = _get_create_table_statement(
            table_name,
            schema,
            partition_by=partition_by,
            location=location,
            table_format=table_format,
        )

        self.hc.sql(create_table_sql)

        if properties:
            table_manager = Table(self, table_name)
            for key, val in properties.items():
                table_manager.set_property(key, val)

        if partition_by:
            self.hc.sql('MSCK REPAIR TABLE {}'.format(table_name))

        return Table(self, table_name)

    def replace_table(self, table_name, schema, location, partition_by=None, table_format=None):
        """Replaces table `table_name` with data represented by schema, location.

        Args:
            table_name (str): Table name.
            schema (pyspark.sql.dataframe.DataFrame): schema.
            location (str): data location, ex.: s3://path/tp/data.
            partition_by (list): fields the data partitioned by.

        Returns:
            Table
        """
        old_table = '{}_OLD'.format(table_name)
        temp_table = '{}_NEW'.format(table_name)

        table_manager = Table(self, table_name)
        old_table_props = table_manager.get_all_properties()

        self.create_table(
            temp_table,
            schema,
            location=location,
            partition_by=partition_by,
            properties=old_table_props,
            table_format=table_format,
        )

        self.hc.sql("""
          ALTER TABLE {} RENAME TO {}
        """.format(table_name, old_table))

        self.hc.sql("""
          ALTER TABLE {} RENAME TO {}
        """.format(temp_table, table_name))

        self.hc.sql("""
          DROP TABLE {}
        """.format(old_table))

        return self.table(old_table)


class Table(object):
    """Represents a table in HiveMetastore.

    Provides meta data operations on a Table.
    """

    def __init__(self, hms, table_name):
        self.hms = hms
        self.hc = hms.hc
        self.table_name = table_name

    def exists(self):
        """Checks if table exists.

        Returns:
            bool
        """
        return self.table_name in self.hms.get_all_tables()

    def set_property(self, name, value):
        """Sets table property.

        Args:
            name (str): Name of the property.
            value (str): Value of the proporty.

        Returns:
            Table: Self.
        """
        self.hc.sql("ALTER TABLE {} SET TBLPROPERTIES ('{}'='{}')".format(
            self.table_name, name, value
        ))
        return self

    def get_property(self, name, to_type=None):
        """Gets table property.

        Args:
            name (str): Name of the property.
            to_type (type): Type to coarce to, str by default.

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
            dict: Property names to values.
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


def _get_create_table_statement(table_name,
                                schema,
                                location,
                                partition_by=None,
                                table_format=None,
                                ):
    """Converts pyspark schema to hive CREATE TABLE definition.

    Args:
        table_name (str): Name of a table.
        schema (dict|pyspark.sql.dataframe.DataFrame|pyspark.sql.types.StructType): \
            Source of schema. Dict should be in format of result of method DataFrame.jsonValue()
        location (str): Table data path, s3 bucket path (or hdfs if you like).
        partition_by (list|None): List of partitioning fields.
        format (str): Format of tables data files.

    Returns
        str: Create table statement.
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
