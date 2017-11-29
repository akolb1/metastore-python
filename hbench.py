#!/usr/local/bin/python3

from __future__ import print_function

import argparse
import logging
from getpass import getuser

from os import path as ospath
from sys import stdout

from benchmarks import benchmark_list_databases
from hmsclient import HMSClient
from microbench import MicroBench
from benchsuite import BenchSuite

_default_host = 'localhost'
_default_port = 9083
_LIST_COMMAND = 'list'


def main():

    parser = argparse.ArgumentParser(description='Hive Metastore client')
    parser.add_argument('-H', '--host', default=_default_host,
                        help='HMS server address')
    parser.add_argument('-d', '--db', help='database name', default=getuser()+'_test')
    parser.add_argument('-L', '--loglevel', help='Log level', default='error',
                        choices=['info', 'debug', 'warning', 'error'])
    parser.add_argument('-W', '--warmup', default=15, type=int, help='Warmup cycles')
    parser.add_argument('-B', '--benchmark', default=100, type=int, help='Benchmark cycles')
    parser.add_argument('-o', '--output', default=stdout, type=argparse.FileType('w'), help='output file')
    parser.add_argument('--sanitize', action='store_true', help='Sanitize results')
    parser.add_argument('--savedata', help='location for raw benchmark data')
    parser.add_argument('--delimiter', help='delimiter for CSV files')
    parser.add_argument('--csv', action='store_true', help='Produce CSV output')
    parser.add_argument('-u', '--user', help='user name', default=getuser())
    parser.add_argument('-v', '--verbose', action='store_true', help='show more information')

    args = parser.parse_args()

    numeric_level = getattr(logging, args.loglevel.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % args.loglevel)
    logging.basicConfig(level=numeric_level)

    logger = logging.getLogger(__name__)
    logger.info('Running benchmark to %s using %d warmup and %d benchmark cycles',
                 args.host, args.warmup, args.benchmark)

    bench = MicroBench(args.warmup, args.benchmark)
    suite = BenchSuite(bench, 1000, sanitize=args.sanitize)

    with HMSClient(args.host) as client:
        setup(client, args)
        try:
            suite.add('listDb', lambda b: benchmark_list_databases(client, b))
            suite.run()
            if args.csv or args.delimiter:
                suite.print_csv(args.output, args.delimiter if args.delimiter else '\t')
            else:
                suite.print(args.output)

            if args.savedata:
                results = suite.result
                for name in sorted(results.keys()):
                    save_data(ospath.join(args.savedata, name + ".out"), results[name].data)
        finally:
            cleanup(client, args)

    return 0


def save_data(name, data):
    with open(name, "w") as f:
        for v in data:
            f.write(str(v) + "\n")


def setup(client, args):
    """
    Set up benchmarking

    :param client: HMS client
    :type client: HMSClient
    :param args: Parameters
    """
    db = args.db
    logger = logging.getLogger(__name__)

    if db not in client.get_all_databases():
        logger.debug("creating database %s", db)
        client.create_database(db, args.user)


def cleanup(client, args):
    """
    Clean up

    :param client: HMS client
    :type client: HMSClient 
    :param args: Parameters
    """
    db = args.db
    logger = logging.getLogger(__name__)
    if db in client.get_all_databases():
        logger.debug("dropping database %s", db)
        client.drop_database(db)


if __name__ == "__main__":
    exit(main())