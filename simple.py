#!/usr/local/bin/python3

from hmsclient import HMSClient

host = 'localhost'
port = '9083'

hms = HMSClient(host, port)
hms.open()
databases = hms.get_all_databases()
for d in databases:
    print(d)
hms.close()

exit(0)

'''
database_pattern = '*'
table_pattern = '*'

# Make socket
transport = TSocket.TSocket(host, int(port))

# Buffering is critical. Raw sockets are very slow
transport = TTransport.TBufferedTransport(transport)

# Wrap in a protocol
protocol = TBinaryProtocol.TBinaryProtocol(transport)

# Create a client to use the protocol encoder
client = ThriftHiveMetastore.Client(protocol)

# Connect!
transport.open()

for d in client.get_databases(database_pattern):
    print '[%s]' % d
    for t in client.get_tables(d, table_pattern):
        table = client.get_table(d, t)
        print ' ' * 4, "{namespace}.{name}:    {location}".format(namespace=d, name=t, location=table.sd.location)
        for c in table.sd.cols:
            print ' ' * 8, c

transport.close()
'''