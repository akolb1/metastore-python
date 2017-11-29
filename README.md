Simple CLI client for HMS Metastore.

# Usage

        usage: hclient.py [-h] [-H HOST] [-d DB] [-t TABLE] [-C COLUMN] [-P PARTITION]
                          [-u USER] [-c COMMENT] [-L {info,debug,warning,error}] [-v]
                          {listdb,list,create,drop}
        
        Hive Metastore client
        
        positional arguments:
          {listdb,list,create,drop}
                                HMS action
        
        optional arguments:
          -h, --help            show this help message and exit
          -H HOST, --host HOST  HMS server address
          -d DB, --db DB        database name
          -t TABLE, --table TABLE
                                table name
          -C COLUMN, --column COLUMN
                                column name:type
          -P PARTITION, --partition PARTITION
                                partition name:type
          -u USER, --user USER  user name
          -c COMMENT, --comment COMMENT
                                comment
          -L {info,debug,warning,error}, --loglevel {info,debug,warning,error}
                                Log level
          -v, --verbose         show more information

# Examples
