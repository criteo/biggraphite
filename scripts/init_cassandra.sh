#!/bin/sh

cd $(git rev-parse --show-toplevel)
source scripts/cassandra_env.sh

${CASSANDRA_HOME}/bin/cqlsh < share/schema.cql
bgutil syncdb

echo "Init Cassandra done"
