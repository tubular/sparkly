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

from collections import OrderedDict
import os
import re
import inspect

try:
    from kafka import SimpleClient
    from kafka.structs import OffsetRequestPayload
except ImportError:
    pass
from pyspark.sql import types as T

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
    """Generate schema by its string definition.

    It's basically an opposite action to `DataType.simpleString` method.
    Supports all atomic types (like string, int, float...) and complex types (array, map, struct)
    except DecimalType.

    Usages:
        >>> parse_schema('string')
        StringType
        >>> parse_schema('int')
        IntegerType
        >>> parse_schema('array<int>')
        ArrayType(IntegerType,true)
        >>> parse_schema('map<string,int>')
        MapType(StringType,IntegerType,true)
        >>> parse_schema('struct<a:int,b:string>')
        StructType(List(StructField(a,IntegerType,true),StructField(b,StringType,true)))
        >>> parse_schema('unsupported')
        Traceback (most recent call last):
        ...
        sparkly.exceptions.UnsupportedDataType: Cannot parse type from string: "unsupported"
    """
    field_type, args_string = re.match('(\w+)<?(.*)>?$', schema).groups()
    args = _parse_args(args_string) if args_string else []

    if field_type in ATOMIC_TYPES:
        return ATOMIC_TYPES[field_type]()
    elif field_type in COMPLEX_TYPES:
        return COMPLEX_TYPES[field_type](*args)
    else:
        message = 'Cannot parse type from string: "{}"'.format(field_type)
        raise UnsupportedDataType(message)


def _parse_args(args_string):
    args = []
    balance = 0
    pos = 0
    for i, ch in enumerate(args_string):
        if ch == '<':
            balance += 1
        elif ch == '>':
            balance -= 1
        elif ch == ',' and balance == 0:
            args.append(args_string[pos:i])
            pos = i + 1

    args.append(args_string[pos:])

    return args


def _is_atomic_type(obj):
    return inspect.isclass(obj) and issubclass(obj, T.AtomicType) and obj is not T.DecimalType


ATOMIC_TYPES = {
    _type[1]().simpleString(): _type[1]
    for _type in inspect.getmembers(T, _is_atomic_type)
}


def _init_map(*args):
    return T.MapType(
        keyType=parse_schema(args[0]),
        valueType=parse_schema(args[1]),
    )


def _init_struct(*args):
    struct = T.StructType()
    for item in args:
        field_name, field_type = item.split(':', 1)
        field_type = parse_schema(field_type)
        struct.add(field_name, field_type)

    return struct


def _init_array(*args):
    return T.ArrayType(parse_schema(args[0]))


COMPLEX_TYPES = {
    'map': _init_map,
    'struct': _init_struct,
    'array': _init_array,
}
