#!/bin/sh

# Run a local Cassandra instance for testing purpose.
#
# Usage:
#  % ./run_cassandra.sh
#

cd $(git rev-parse --show-toplevel)
source scripts/cassandra_env.sh

${CASSANDRA_HOME}/bin/cassandra
