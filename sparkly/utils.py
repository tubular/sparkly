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

import os
from collections import OrderedDict

try:
    from kafka import SimpleClient
    from kafka.structs import OffsetRequestPayload
except ImportError:
    pass
from pyspark.sql.types import (StructType, StringType, LongType, IntegerType,
                               FloatType, BooleanType, MapType, ArrayType)

from sparkly.exceptions import UnsupportedDataType


def absolute_path(file_path, *rel_path):
    """Return absolute path to file.

    Usage:
        >>> absolute_path('/my/current/dir/x.txt', '..', 'x.txt')
        '/my/current/x.txt'

        >>> absolute_path('/my/current/dir/x.txt', 'relative', 'path')
        '/my/current/dir/relative/path'

        >>> import os
        >>> absolute_path('x.txt', 'relative/path') == os.getcwd() + '/relative/path'
        True

    Args:
        file_path (str): file
        rel_path (list[str]): path parts

    Returns:
        str
    """
    return os.path.abspath(
        os.path.join(
            os.path.dirname(
                os.path.realpath(file_path)
            ),
            *rel_path
        )
    )


def kafka_get_topics_offsets(host, topic, port=9092):
    """Return available partitions and their offsets for the given topic.

    Args:
        host (str): Kafka host.
        topic (str): Kafka topic.
        port (int): Kafka port.

    Returns:
        [(int, int, int)]: [(partition, start_offset, end_offset)].
    """
    brokers = ['{}:{}'.format(host, port)]
    client = SimpleClient(brokers)

    offsets = []
    partitions = client.get_partition_ids_for_topic(topic)

    offsets_responses_end = client.send_offset_request(
        [OffsetRequestPayload(topic, partition, -1, 1)
         for partition in partitions]
    )
    offsets_responses_start = client.send_offset_request(
        [OffsetRequestPayload(topic, partition, -2, 1)
         for partition in partitions]
    )

    for start_offset, end_offset in zip(offsets_responses_start, offsets_responses_end):
        offsets.append((start_offset.partition,
                        start_offset.offsets[0],
                        end_offset.offsets[0]))

    return offsets


def parse_schema(schema):
    """Converts string to Spark schema definition.

    Usages:
        >>> parse('struct[a:struct[a:string]]').simpleString()
        'struct<a:struct<a:string>>'

    Args:
        schema (str): Schema definition as string.

    Returns:
        StructType

    Raises:
        UnsupportedDataType: In case of unsupported data type.
    """
    return _generate_structure_type(_parse_schema(schema))


def _generate_structure_type(fields_and_types):
    """Generate a StructType from the dict of fields & types.

    Schema definition supports basic types: string, integer, long, float, boolean.
    And complex types in any combinations: dict, struct, list.

    Usages:
        >>> _generate_structure_type({'field_a': 'long'}).simpleString()
        'struct<field_a:bigint>'
        >>> _generate_structure_type({'field_a': 'dict[string,long]'}).simpleString()
        'struct<field_a:map<string,bigint>>'
        >>> _generate_structure_type({'field_a': 'loooong'})
        Traceback (most recent call last):
        ...
        sparkly.exceptions.UnsupportedDataType: Unsupported type field_a for field loooong

    Args:
        fields_and_types (dict[str, str]): Field - type associations.
            Possible types are string, integer, long, float, boolean, dict, struct.

    Returns:
        StructType

    Raises:
        UnsupportedDataType: In case of unsupported data type.
    """
    struct = StructType()
    for field_name, field_type in fields_and_types.items():
        try:
            struct.add(field_name, _process_type(field_type))
        except UnsupportedDataType:
            message = 'Unsupported type {} for field {}'.format(field_type, field_name)
            raise UnsupportedDataType(message)

    return struct


def _parse_schema(schema):
    """Converts schema string to dict: field_name -> type definition string.

    Note:
        We need an OrderedDict here because ordering matters in Dataframe definition
        when applied to a RDD, like when we read from Kafka RDDs.

    Example:
        name:string|age:int -> {'name': 'string', 'age': 'int'}

    Args:
        schema (str): schema as string

    Returns:
        (OrderedDict)
    """
    return OrderedDict(x.split(':', 1) for x in schema.strip('|').split('|'))


TYPES = {
    'string': StringType,
    'integer': IntegerType,
    'long': LongType,
    'float': FloatType,
    'boolean': BooleanType,
}


def _init_dict(*args):
    return MapType(keyType=_process_type(args[0]),
                   valueType=_process_type(args[1]))


def _init_struct(*args):
    struct = StructType()
    for item in args:
        field_name, field_type = item.split(':', 1)
        field_type = _process_type(field_type)
        struct.add(field_name, field_type)

    return struct


def _init_list(*args):
    return ArrayType(_process_type(args[0]))


COMPLEX_TYPES = {
    'dict': _init_dict,
    'struct': _init_struct,
    'list': _init_list,
}


def _process_type(field_type):
    """Generate schema recursively by string definition.

    It supports basic types: string, integer, long, float, boolean.
    And complex types in any combinations: dict, struct, list.

    Usages:
        >>> _process_type('string').simpleString()
        'string'
        >>> _process_type('list[dict[string,string]]').simpleString()
        'array<map<string,string>>'
        >>> _process_type('struct[a:struct[a:string]]').simpleString()
        'struct<a:struct<a:string>>'
        >>> _process_type('dict[string,struct[a:dict[long,string]]]').simpleString()
        'map<string,struct<a:map<bigint,string>>>'
        >>> _process_type('struct[a:dict[long,dict[string,long]],'
        ...               'c:dict[long,string]]').simpleString()
        'struct<a:map<bigint,map<string,bigint>>,c:map<bigint,string>>'
        >>> _process_type('map[string,long]')
        Traceback (most recent call last):
        ...
        sparkly.exceptions.UnsupportedDataType: Cannot parse type from string: "map[string,long]"
    """
    if field_type in TYPES:
        return TYPES[field_type]()
    else:
        for key in COMPLEX_TYPES:
            if field_type.startswith(key):
                str_args = field_type[len(key) + 1:-1]
                blnc = 0
                pos = 0
                args = []
                for i, ch in enumerate(str_args):
                    if ch == '[':
                        blnc += 1
                    if ch == ']':
                        blnc -= 1
                    if ch == ',' and blnc == 0:
                        args.append(str_args[pos:i])
                        pos = i + 1

                args.append(str_args[pos:])

                return COMPLEX_TYPES[key](*args)

    message = 'Cannot parse type from string: "{}"'.format(field_type)
    raise UnsupportedDataType(message)
