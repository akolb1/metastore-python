#!/usr/local/bin/python3

from __future__ import print_function

import argparse
import logging
from sys import stderr
from getpass import getuser

import re

from hive_metastore.ttypes import AlreadyExistsException
from hmsclient import HMSClient

_default_host = 'localhost'
_default_port = 9083
_LIST_COMMAND = 'list'


def main():
    parser = argparse.ArgumentParser(description='Hive Metastore client')
    parser.add_argument('-H', '--host', dest='host', default=_default_host,
                        help='HMS server address')
    parser.add_argument('-d', '--db', help='database name')
    parser.add_argument('-t', '--table', help='table name')
    parser.add_argument('-C', '--column', action='append', help='column name:type')
    parser.add_argument('-P', '--partition', action='append', help='partition name:type')
    parser.add_argument('-u', '--user', help='user name', default=getuser())
    parser.add_argument('-c', '--comment', help='comment')
    parser.add_argument('-L', '--loglevel', help='Log level', default='error',
                        choices=['info', 'debug', 'warning', 'error'])
    parser.add_argument('-v', '--verbose',
                        action='store_true',
                        help='show more information')
    parser.add_argument('command',
                        choices=['listdb', 'list', 'create', 'drop'],
                        help='HMS action')

    args = parser.parse_args()

    numeric_level = getattr(logging, args.loglevel.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % args.loglevel)
    logging.basicConfig(level=numeric_level)

    logger = logging.getLogger(__name__)

    with HMSClient(args.host) as client:
        if args.command == 'listdb':
            for d in client.get_all_databases():
                if not args.db or re.search(args.db, d):
                    print(d)
            return 0

        if args.command == 'list':
            for db in client.get_all_databases():
                if not args.db or re.search(args.db, db):
                    for t in client.get_all_tables(db):
                        if not args.table or re.search(args.table, t):
                            print('{}.{}'.format(db, t))

            return 0

        if args.command == 'create':
            try:
                if args.table:
                    logger.debug('creating table %s.%s as user %s', args.table, args.db, args.user)
                    client.create_table(client.make_table(args.db,
                                                          args.table,
                                                          owner=args.user,
                                                          columns=client.make_schema(args.column),
                                                          partition_keys=client.make_schema(args.partition)))
                else:
                    logger.debug('creating database %s', args.db)
                    client.create_database(args.db, args.comment, args.user)
            except AlreadyExistsException:
                logger.error("Object %s.%s already exists", args.db, args.table)
                stderr.write("Object {}.{} already exists\n".format(args.db, args.table))
                return 1

            return 0

    return 0


if __name__ == "__main__":
    exit(main())
