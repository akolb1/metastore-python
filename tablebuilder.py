from hive_metastore.ttypes import StorageDescriptor, SerDeInfo, Table


class TableBuilder(object):
    """
    Build Table object
    """

    SIMPLE_SERDE = 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
    INPUT_FORMAT = 'org.apache.hadoop.mapred.TextInputFormat'
    OUTPUT_FORMAT = 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'

    MANAGED_TABLE = 'MANAGED_TABLE'
    EXTERNAL_TABLE = 'EXTERNAL_TABLE'
    VIRTUAL_VIEW = 'VIRTUAL_VIEW'
    INDEX_TABLE = 'INDEX_TABLE'

    def __init__(self, db_name, table_name):
        """
        Create TableBuilder

        :param db_name:
        :type db_name: str
        :param table_name:
        :type table_name: str
        """

        self.db_name = db_name
        self.table_name = table_name
        self.owner = ''
        self.columns = []
        self.serde = self.SIMPLE_SERDE
        self.input_format = self.INPUT_FORMAT
        self.output_format = self.OUTPUT_FORMAT
        self.partition_keys = None
        self.table_type = self.MANAGED_TABLE

    def build(self):
        sd = StorageDescriptor(
            cols=self.columns,
            serdeInfo=SerDeInfo(name=self.table_name,
                                serializationLib=self.serde),
            inputFormat=self.input_format,
            outputFormat=self.output_format
        )
        return Table(tableName=self.table_name, dbName=self.db_name,
                     owner=self.owner,
                     tableType=self.table_type,
                     partitionKeys=self.partition_keys,
                     sd=sd)

    def set_owner(self, owner):
        """
        Set table owner

        :param owner: table owner
        :type owner: str
        :return: self
        """
        self.owner = owner
        return self

    def set_columns(self, columns):
        self.columns = columns
        return self

    def set_serde(self, serde):
        self.serde = serde
        return self

    def set_input_format(self, input_format):
        self.input_format = input_format
        return self

    def set_output_format(self, output_format):
        self.output_format = output_format
        return self

    def set_partition_keys(self, partition_keys):
        self.partition_keys = partition_keys
        return self

    def set_table_type(self, table_type):
        self.table_type = table_type
        return self
