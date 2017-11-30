import copy
import logging

from thrift.protocol import TBinaryProtocol
from thrift.transport import TSocket, TTransport

from hive_metastore import ThriftHiveMetastore
from hive_metastore.ttypes import Database, StorageDescriptor, SerDeInfo, Table, FieldSchema, Partition

SIMPLE_SERDE = 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
INPUT_FORMAT = 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUT_FORMAT = 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
DEFAULT_PORT = 9083


class HMSClient(object):
    __client = None
    __transport = None
    __isOpened = False

    def __init__(self, host, port=DEFAULT_PORT):
        self.__transport = TTransport.TBufferedTransport(TSocket.TSocket(host, int(port)))
        protocol = TBinaryProtocol.TBinaryProtocol(self.__transport)
        self.__client = ThriftHiveMetastore.Client(protocol)
        self.logger = logging.getLogger(__name__)


    def open(self):
        self.__transport.open()
        self.__isOpened = True
        return self

    def __enter__(self):
        self.open()
        return self

    def close(self):
        self.__transport.close()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def get_all_databases(self):
        return self.__client.get_all_databases()

    def get_all_tables(self, db_name):
        return self.__client.get_all_tables(db_name=db_name)

    def create_database(self, db_name, comment=None, owner=None):
        """
        Create database

        :param db_name: database name
        :type db_name: str
        :param comment: database comment
        :type comment: str
        :param owner: database user
        :type owner: str
        """
        self.logger.debug('create_database(%s, %s, %s)', db_name, comment, owner)
        self.__client.create_database(Database(name=db_name, description=comment, ownerName=owner))

    def drop_database(self, db_name):
        """
        Drop database

        :param db_name: Database name
        :type db_name: str
        """
        self.__client.drop_database(db_name, deleteData=True, cascade=False)

    @staticmethod
    def make_table(db_name, table_name, owner='', columns=None, partition_keys=None):
        """
        Create Table object

        :param db_name: Database name
        :type db_name: str
        :param table_name: Table name
        :type table_name: str
        :param columns:
        :param owner: Table owner
        :type owner: str
        :param partition_keys:
        :return: Table object
        :rtype: Table
        """
        sd = StorageDescriptor(
            cols=columns if columns else [],
            serdeInfo=SerDeInfo(name=table_name, serializationLib=SIMPLE_SERDE),
            inputFormat=INPUT_FORMAT,
            outputFormat=OUTPUT_FORMAT
        )
        return Table(tableName=table_name, dbName=db_name, owner=owner,
                     partitionKeys=partition_keys,
                     sd=sd)

    @staticmethod
    def make_schema(params):
        """
        Produce field schema from list of parameters

        :param params: list of parameters or tuples
        :type params: list[str]
        :return: resulting field schema
        :rtype: list[FieldSchema]
        """
        schema = []
        for param in params:
            param_type = 'string'
            if ':' in param:
                parts = param.split(':')
                param_name = parts[0]
                param_type = parts[1] if parts[1] else 'string'
            else:
                param_name = param

            schema.append(FieldSchema(name=param_name, type=param_type, comment=''))

        return schema

    @staticmethod
    def parse_schema(schemas):
        """
        Convert list of FieldSchema objects in a list of name:typ strings

        :param schemas:
        :type schemas: list[FieldSchema]
        :return:
        """
        return map(lambda s: '{}\t{}'.format(s.name, s.type), schemas)

    def create_table(self, table):
        self.__client.create_table(table)


    def drop_table(self, db_name, table_name):
        """
        Drop Hive table

        :param db_name: Database name
        :type db_name: str
        :param table_name: Table Name
        :type table_name: str
        """
        self.__client.drop_table(db_name, table_name, True)

    def get_table(self, db_name, table_name):
        """
        Get table information

        :param db_name: Database name
        :type db_name: str
        :param table_name: Table name
        :type table_name: str
        :return: Table info
        :rtype: Table
        """
        return self.__client.get_table(db_name, table_name)
        pass

    @staticmethod
    def make_partition(table, values):
        """

        :param table:
        :type table: Table
        :param values:
        :type values: list[str]
        :return:
        :rtype: Partition
        """
        partition_names = [k.name for k in table.partitionKeys]
        if len(partition_names) != len(values):
            raise ValueError('Partition values do not match table schema')
        kv = [partition_names[i] + '=' + values[i] for i in range(len(partition_names))]

        sd = copy.deepcopy(table.sd)
        sd.location = sd.location + '/' + '/'.join(kv)

        return Partition(values=values, dbName=table.dbName, tableName=table.tableName, sd=sd)

    def add_partition(self, table, values):
        """
        Add partition

        :param table:
        :type table: Table
        :param values:
        :type values: list[str]
        """
        self.__client.add_partition(self.make_partition(table, values))

    def add_partitions(self, partitions):
        self.__client.add_partitions(partitions)

    def drop_partition(self, db_name, table_name, values):
        self.__client.drop_partition(db_name, table_name, values, True)