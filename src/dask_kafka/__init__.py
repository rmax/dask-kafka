# -*- coding: utf-8 -*-
import random

from dask import delayed

from .scanner import KafkaScannerDirect


__author__ = 'Rolando (Max) Espinoza'
__email__ = 'rolando@rmax.io'
__version__ = '0.1.0-dev'


def read_kafka(brokers, topic, batch_size, group=None, partitions=None,
               start_offsets=None, stop_offsets=None, kafka_options=None):
    """Read a kafka topic in batches.
    """
    kafka_options = kafka_options or {}

    if not group:
        group = 'read-kafka-%s' % topic

    if stop_offsets is None:
        stop_offsets = get_latest_offsets(brokers, topic, group=group, **kafka_options)

    if start_offsets is None:
        start_offsets = {}

    if partitions is None:
        partitions = set(stop_offsets.keys())

    for partition in partitions:
        start_offsets.setdefault(partition, 0)
        if partition not in stop_offsets:
            raise ValueError("Partition '%s' with no value in stop_offsets" % partition)

    values = []
    for partition in partitions:
        early_offset = start_offsets[partition]
        latest_offset = stop_offsets[partition]
        for from_offset in range(early_offset, latest_offset, batch_size):
            until_offset = min(from_offset + batch_size, latest_offset)
            values.append(delayed(_read_kafka)(
                brokers, topic, partition, from_offset, until_offset, group=group, **kafka_options
            ))
    # XXX: Use restrictions instead of random shuffle.
    random.shuffle(values)

    return values


def _read_kafka(brokers, topic, partition, from_offset, until_offset,
                group=None, max_fetch_bytes=2**20,
                scanner_cls=KafkaScannerDirect, **kwargs):
    """Read messages from a given partition offset range.
    """
    range_size = until_offset - from_offset
    range_scanner = scanner_cls(
        brokers=brokers,
        topic=topic,
        group=group,
        keep_offsets=False,
        partitions={partition, },
        start_offsets={partition: from_offset},
        stop_offsets={partition: until_offset},
        batchsize=range_size,
        max_next_messages=range_size,
        batch_autocommit=False,
        ssl_configs=kwargs,
        max_partition_fetch_bytes=max_fetch_bytes,
    )
    messages = []
    for batch in range_scanner.scan_topic_batches():
        messages.extend(batch)
    return messages


def get_latest_offsets(brokers, topic, group=None, scanner_cls=KafkaScannerDirect, **kwargs):
    """Returns latest offsets by partitions.
    """
    scanner = scanner_cls(brokers, topic, group=group, keep_offsets=False, ssl_configs=kwargs)
    return scanner.latest_offsets
