# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Actual benchmarks
"""
import logging

import copy

from hmsclient import HMSClient
from tablebuilder import TableBuilder


def benchmark_list_databases(client, bench):
    return bench.bench_simple(lambda: client.get_all_databases())


def benchmark_create_table(client, bench, db, table_name, owner):
    """
    Benchmark table creation

    :param client:
    :type client: HMSClient
    :param bench:
    :type bench: MicroBench
    :param db:
    :type db: str
    :param table_name:
    :type table_name: str
    :param owner: table owner
    :type owner: str
    """
    schema = HMSClient.make_schema(['name'])
    table = TableBuilder(db, table_name)\
        .set_owner(owner)\
        .set_columns(schema)\
        .build()

    return bench.bench(
        None,
        lambda: client.create_table(table),
        lambda: client.drop_table(db, table_name)
    )


def benchmark_drop_table(client, bench, db, table_name, owner):
    """
    Benchmark table drops

    :param client:
    :type client: HMSClient
    :param bench:
    :type bench: MicroBench
    :param db:
    :type db: str
    :param table_name:
    :type table_name: str
    :param owner: table owner
    :type owner: str
    """
    schema = HMSClient.make_schema(['name'])
    table = TableBuilder(db, table_name)\
        .set_owner(owner)\
        .set_columns(schema)\
        .build()

    return bench.bench(
        lambda: client.create_table(table),
        lambda: client.drop_table(db, table_name),
        None
    )


# noinspection SpellCheckingInspection
def benchmark_list_tables(client, bench, db, table_name, owner, ntables):
    """
    Create ntables and measure time listing them
    :param client:
    :param bench:
    :param db:
    :param table_name:
    :param owner:
    :param ntables:
    """
    _create_many_tables(client, db, table_name, owner, ntables)
    try:
        return bench.bench_simple(lambda: client.get_all_tables(db))
    finally:
        _drop_many_tables(client, db, table_name, ntables)


def benchmark_get_table(client, bench, db, table_name, owner):
    _create_many_tables(client, db, table_name, owner, 1)
    try:
        return bench.bench_simple(lambda: client.get_table(db, table_name + '_0'))
    finally:
        _drop_many_tables(client, db, table_name, 1)


# noinspection SpellCheckingInspection
def _create_many_tables(client, db, table_name, owner, ntables):
    schema = HMSClient.make_schema(['name'])
    logger = logging.getLogger(__name__)
    logger.debug("creating %d tables for %s.%s", ntables, db, table_name)
    for i in range(ntables):
        table = TableBuilder(db, '{}_{}'.format(table_name, i)) \
            .set_owner(owner) \
            .set_columns(schema) \
            .build()
        client.create_table(table)


def _drop_many_tables(client, db, table_name, ntables):
    logger = logging.getLogger(__name__)
    logger.debug("dropping %d tables for %s.%s", ntables, db, table_name)
    for i in range(ntables):
        client.drop_table(db, '{}_{}'.format(table_name, i))


def benchmark_add_partition(client, bench, db, table_name, owner):
    logger = logging.getLogger(__name__)
    schema = HMSClient.make_schema(['name'])
    part_schema = HMSClient.make_schema(['date'])
    logger.debug("creating table %s.%s", db, table_name)
    table = TableBuilder(db, table_name)\
        .set_owner(owner)\
        .set_columns(schema)\
        .set_partition_keys(part_schema)\
        .build()
    client.create_table(table)

    tbl = client.get_table(db, table_name)
    values = ["d"]
    try:
        return bench.bench(
            None,
            lambda: client.add_partition(tbl, values),
            lambda: client.drop_partition(db, table_name, values)
        )
    finally:
        logger.debug("dropping table %s.%s", db, table_name)
        client.drop_table(db, table_name)


def benchmark_drop_partition(client, bench, db, table_name, owner):
    logger = logging.getLogger(__name__)
    schema = HMSClient.make_schema(['name'])
    part_schema = HMSClient.make_schema(['date'])
    logger.debug("creating table %s.%s", db, table_name)
    table = TableBuilder(db, table_name)\
        .set_owner(owner)\
        .set_columns(schema)\
        .set_partition_keys(part_schema)\
        .build()
    client.create_table(table)

    tbl = client.get_table(db, table_name)
    values = ["d"]
    try:
        return bench.bench(
            lambda: client.add_partition(tbl, values),
            lambda: client.drop_partition(db, table_name, values),
            None
        )
    finally:
        logger.debug("dropping table %s.%s", db, table_name)
        client.drop_table(db, table_name)


def benchmark_add_partitions(client, bench, db, table_name, owner, count):
    logger = logging.getLogger(__name__)
    schema = HMSClient.make_schema(['name'])
    part_schema = HMSClient.make_schema(['date'])
    logger.debug("creating table %s.%s", db, table_name)
    table = TableBuilder(db, table_name)\
        .set_owner(owner)\
        .set_columns(schema)\
        .set_partition_keys(part_schema)\
        .build()
    client.create_table(table)

    tbl = client.get_table(db, table_name)
    partitions = [client.make_partition(tbl, ["d" + str(i)]) for i in range(count)]
    try:
        return bench.bench(
            None,
            lambda: client.add_partitions(partitions),
            lambda: client.drop_all_partitions(db, table_name)
        )
    finally:
        logger.debug("dropping table %s.%s", db, table_name)
        client.drop_table(db, table_name)


def benchmark_drop_partitions(client, bench, db, table_name, owner, count, need_result=None):
    logger = logging.getLogger(__name__)
    schema = HMSClient.make_schema(['name'])
    part_schema = HMSClient.make_schema(['date'])
    logger.debug("creating table %s.%s", db, table_name)
    table = TableBuilder(db, table_name)\
        .set_owner(owner)\
        .set_columns(schema)\
        .set_partition_keys(part_schema)\
        .build()
    client.create_table(table)

    tbl = client.get_table(db, table_name)
    names = ["date=d" + str(i) for i in range(count)]
    print(names)

    partitions = [client.make_partition(tbl, ["d" + str(i)]) for i in range(count)]
    try:
        return bench.bench(
            lambda: client.add_partitions(partitions),
            lambda: client.drop_partitions(db, names, need_result),
            None
        )
    finally:
        logger.debug("dropping table %s.%s", db, table_name)
        client.drop_table(db, table_name)


def _create_many_partitions(client, db, table_name, owner, count):
    logger = logging.getLogger(__name__)
    logger.debug("creating %d partitions for table %s.%s", count, db, table_name)
    schema = HMSClient.make_schema(['name'])
    part_schema = HMSClient.make_schema(['date'])
    logger.debug("creating table %s.%s", db, table_name)
    table = TableBuilder(db, table_name)\
        .set_owner(owner)\
        .set_columns(schema)\
        .set_partition_keys(part_schema)\
        .build()
    client.create_table(table)
    tbl = client.get_table(db, table_name)
    partitions = [client.make_partition(tbl, ["d" + str(i)]) for i in range(count)]
    client.add_partitions(partitions)


def benchmark_get_partitions(client, bench, db, table_name, owner, count):
    logger = logging.getLogger(__name__)
    _create_many_partitions(client, db, table_name, owner, count)
    try:
        logger.debug("measuring time to list %s partitions", count)
        return bench.bench_simple(lambda: client.get_partitions(db, table_name))
    finally:
        logger.debug("dropping table %s.%s", db, table_name)
        client.drop_table(db, table_name)


def benchmark_get_partition_names(client, bench, db, table_name, owner, count):
    logger = logging.getLogger(__name__)
    _create_many_partitions(client, db, table_name, owner, count)
    try:
        logger.debug("measuring time to get names for %s partitions", count)
        return bench.bench_simple(lambda: client.get_partition_names(db, table_name))
    finally:
        logger.debug("dropping table %s.%s", db, table_name)
        client.drop_table(db, table_name)


def benchmark_get_curr_notification(client, bench):
    return bench.bench_simple(lambda: client.get_current_notification_id())


def benchmark_rename_table(client, bench, db, owner, count):
    logger = logging.getLogger(__name__)
    table_name = "bench_table"
    new_name = table_name + "_renamed"
    _create_many_partitions(client, db, table_name, owner, count)
    table = client.get_table(db, table_name)
    table.sd.location = ""
    new_table = copy.deepcopy(table)
    new_table.tableName = new_name
    try:
        logger.debug("measuring time to rename table with %d partitions", count)
        return bench.bench(
            None,
            lambda: client.alter_table(db, table_name, new_table),
            lambda: client.alter_table(db, new_name, table)
        )
    finally:
        logger.debug("dropping table %s.%s", db, table_name)
        client.drop_table(db, table_name)
