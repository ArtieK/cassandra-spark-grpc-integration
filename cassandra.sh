echo "-Xms128M" >> /apache-cassandra-5.0.6/conf/jvm-server.options
echo "-Xmx128M" >> /apache-cassandra-5.0.6/conf/jvm-server.options

# get the environment variable
PROJECT=${PROJECT}

sed -i "s/^listen_address:.*/listen_address: "`hostname`"/" /apache-cassandra-5.0.6/conf/cassandra.yaml
sed -i "s/^rpc_address:.*/rpc_address: "`hostname`"/" /apache-cassandra-5.0.6/conf/cassandra.yaml
sed -i "s/- seeds:.*/- seeds: ${PROJECT}-db-1,${PROJECT}-db-2,${PROJECT}-db-3/" /apache-cassandra-5.0.6/conf/cassandra.yaml

/apache-cassandra-5.0.6/bin/cassandra -R
sleep infinity