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

try:
    from kafka import SimpleClient
    from kafka.structs import OffsetRequestPayload
except ImportError:
    pass


def absolute_path(file_path, *rel_path):
    """Returns absolute path to file.

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
    """Returns available partitions and their offsets for the given topic.

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
