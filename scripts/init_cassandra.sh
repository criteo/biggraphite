#!/bin/sh

# Init Cassandra database.
# Run this to bootstrap the database or to update its schema. It requires
# Cassandra to be started (see run_cassandra.sh).
#
# Usage:
#  $ ./init_cassandra.sh
#

cd $(git rev-parse --show-toplevel)
source scripts/cassandra_env.sh

${CASSANDRA_HOME}/bin/cqlsh < share/schema.cql
bgutil syncdb

echo "Init Cassandra done"
