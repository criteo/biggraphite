#!/usr/bin/env bash

set -o xtrace
set -o verbose

# On Travis, /tmp is on tmpfs

if [ -n "${CASSANDRA_VERSION}" ]; then
    rm -rf .deps/apache-cassandra-${CASSANDRA_VERSION}/data
    sudo mkdir /tmp/cs-bg-data
    ln -s /tmp/cs-bg-data .deps/apache-cassandra-${CASSANDRA_VERSION}/data
fi

if [ -n "${ES_VERSION}" ]; then
    rm -rf .deps/elasticsearch-${ES_VERSION}/data
    sudo mkdir /tmp/es-bg-data
    ln -s /tmp/es-bg-data .deps/elasticsearch-${ES_VERSION}/data
fi
