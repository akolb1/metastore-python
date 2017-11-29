"""
Actual benchmarks
"""
from hmsclient import HMSClient


def benchmark_demo(bench):
    return  bench.bench(
        lambda: print('Pre'),
        lambda: print('Run'),
        lambda: print('Post')
    )

def benchmark_list_databases(client, bench):
    return bench.bench_simple(lambda : client.get_all_databases())


def benchmark_create_table(client, bench, db, tableName, owner):
    """
    Benchmark table creation

    :param client:
    :type client: HMSClient
    :param bench:
    :type bench: MicroBench
    :param db:
    :type db: str
    :param tableName:
    :type tableName: str
    :param owner: table owner
    :type owner: str
    """
    schema = HMSClient.make_schema(['name'])
    table = HMSClient.make_table(db, tableName, owner = owner, columns=schema)

    return bench.bench(
        None,
        lambda: client.create_table(table),
        lambda: client.drop_table(db, tableName)
    )


def benchmark_drop_table(client, bench, db, tableName, owner):
    """
    Benchmark table drops

    :param client:
    :type client: HMSClient
    :param bench:
    :type bench: MicroBench
    :param db:
    :type db: str
    :param tableName:
    :type tableName: str
    :param owner: table owner
    :type owner: str
    """
    schema = HMSClient.make_schema(['name'])
    table = HMSClient.make_table(db, tableName, owner = owner, columns=schema)

    return bench.bench(
        lambda: client.create_table(table),
        lambda: client.drop_table(db, tableName),
        None
    )
