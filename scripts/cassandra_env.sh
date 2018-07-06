#!/bin/sh

cd $(git rev-parse --show-toplevel)

source scripts/env.sh

export CASSANDRA_VERSION=3.11.2
export CASSANDRA_HOME=$(pwd)/apache-cassandra-${CASSANDRA_VERSION}

if [ ! -d "${CASSANDRA_HOME}" ]; then
    wget "http://www.us.apache.org/dist/cassandra/${CASSANDRA_VERSION}/apache-cassandra-${CASSANDRA_VERSION}-bin.tar.gz"
    tar -xzf "apache-cassandra-${CASSANDRA_VERSION}-bin.tar.gz"
fi
