"""
Actual benchmarks
"""
import logging

from hmsclient import HMSClient


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
    table = HMSClient.make_table(db, table_name, owner=owner, columns=schema)

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
    table = HMSClient.make_table(db, table_name, owner=owner, columns=schema)

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
        return bench.bench_simple(lambda: client.get_table(db, table_name+'_0'))
    finally:
        _drop_many_tables(client, db, table_name, 1)


# noinspection SpellCheckingInspection
def _create_many_tables(client, db, table_name, owner, ntables):
    schema = HMSClient.make_schema(['name'])
    logger = logging.getLogger(__name__)
    logger.debug("creating %d tables for %s.%s", ntables, db, table_name)
    for i in range(ntables):
        table = HMSClient.make_table(db, '{}_{}'.format(table_name, i),
                                     owner=owner, columns=schema)
        client.create_table(table)


def _drop_many_tables(client, db, table_name, ntables):
    logger = logging.getLogger(__name__)
    logger.debug("dropping %d tables for %s.%s", ntables, db, table_name)
    for i in range(ntables):
        client.drop_table(db, '{}_{}'.format(table_name, i))
