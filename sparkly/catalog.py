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


class SparklyCatalog(object):
    """A set of tools to interact with HiveMetastore."""

    def __init__(self, spark):
        """Constructor.

        Args:
            spark (sparkly.SparklySession)
        """
        self._spark = spark

    def drop_table(self, table_name, checkfirst=True):
        """Drop table from the metastore.

        Note:
            Follow the official documentation to understand `DROP TABLE` semantic.
            https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL\
            #LanguageManualDDL-DropTable

        Args:
            table_name (str): A table name.
            checkfirst (bool): Only issue DROPs for tables that are presented in the database.
        """
        drop_statement = 'DROP TABLE IF EXISTS' if checkfirst else 'DROP TABLE'
        return self._spark.sql('{} `{}`'.format(drop_statement, table_name))

    def has_table(self, table_name, db_name=None):
        """Check if table is available in the metastore.

        Args:
            table_name (str): A table name.
            db_name (str): A database name.

        Returns:
            bool
        """
        for table in self._spark.catalog.listTables(db_name):
            if table.name == table_name:
                return True

        return False

    def rename_table(self, old_table_name, new_table_name):
        """Rename table in the metastore.

        Note:
            Follow the official documentation to understand `ALTER TABLE` semantic.
            https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL\
            #LanguageManualDDL-RenameTable

        Args:
            old_table_name (str): The current table name.
            new_table_name (str): An expected table name.
        """
        self._spark.sql('ALTER TABLE `{}` RENAME TO `{}`'.format(old_table_name, new_table_name))

    def get_table_property(self, table_name, property_name, to_type=None):
        """Get table property value from the metastore.

        Args:
            table_name (str): A table name. Might contain a db name.
                E.g. "my_table" or "default.my_table".
            property_name (str): A property name to read value for.
            to_type (function): Cast value to the given type. E.g. `int` or `float`.

        Returns:
            Any
        """
        if not to_type:
            to_type = str

        df = self._spark.sql("SHOW TBLPROPERTIES `{}`('{}')".format(table_name, property_name))
        prop_val = df.collect()[0].value.strip()

        if 'does not have property' not in prop_val:
            return to_type(prop_val)

    def get_table_properties(self, table_name):
        """Get table properties from the metastore.

        Args:
            table_name (str): A table name.

        Returns:
            dict[str,str]: Key/value for properties.
        """
        rows = self._spark.sql('SHOW TBLPROPERTIES `{}`'.format(table_name)).collect()
        return {row.key: row.value for row in rows}

    def set_table_property(self, table_name, property_name, value):
        """Set value for table property.

        Args:
            table_name (str): A table name.
            property_name (str): A property name to set value for.
            value (Any): Will be automatically casted to string.
        """
        self._spark.sql("ALTER TABLE `{}` SET TBLPROPERTIES ('{}'='{}')".format(
            table_name, property_name, value
        ))
