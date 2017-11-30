Simple CLI client for HMS Metastore.

# Usage

    usage: hclient [-h] [-H HOST] [-d DB] [-t TABLE] [-C COLUMN] [-P PARTITION]
                   [-u USER] [-c COMMENT] [-L {info,debug,warning,error}]
                   [--force] [-v]
                   {add,listdb,list,create,drop,rm}
    
    Hive Metastore client
    
    positional arguments:
      {add,listdb,list,create,drop,rm}
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
      --force               force destructive operation
      -v, --verbose         show more information


# Examples

## List databases

### List all databases

     hclient -H host listdb

### List databases containing 'foo' in the name

     hclient -H host -d foo listdb

## List tables

### List all tables

    hclient -H host list
    
### List tables containing 'foo' in the name

    hclient -H host -t foo list
