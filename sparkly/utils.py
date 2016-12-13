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


def kafka_get_topics_offsets(host, topic, port=None):
    """Returns available partitions and its offsets for list of topics.

    Args:
        host (str): Kafka host.
        topic (str): Kafka topic.
        port (int|None): Kafka port.
    Returns:
        [(int, int, int)]: [(partition, start_offset, end_offset)].
    """
    brokers = ['{}:{}'.format(host, port or 9092)]
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
