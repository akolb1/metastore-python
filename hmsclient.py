from thrift.protocol import TBinaryProtocol
from thrift.transport import TSocket, TTransport

from hive_metastore import ThriftHiveMetastore
from hive_metastore.ttypes import Database, StorageDescriptor, SerDeInfo, Table, FieldSchema

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

    def create_table(self, table):
        self.__client.create_table(table)
