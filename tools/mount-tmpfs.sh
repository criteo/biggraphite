#!/usr/bin/env bash

if [ -n "${CASSANDRA_VERSION}" ]; then
    echo "Mounting Cassandra data directory to tmpfs"
    rm -rf .deps/apache-cassandra-${CASSANDRA_VERSION}/data
    mkdir -p /dev/shm/cs-bg-data
    ln -s /dev/shm/cs-bg-data .deps/apache-cassandra-${CASSANDRA_VERSION}/data
fi

if [ -n "${ES_VERSION}" ]; then
    echo "Mounting Elasticsearch data directory to tmpfs"
    rm -rf .deps/elasticsearch-${ES_VERSION}/data
    mkdir -p /dev/shm/es-bg-data
    ln -s /dev/shm/es-bg-data .deps/elasticsearch-${ES_VERSION}/data
fi
