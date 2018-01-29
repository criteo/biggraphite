#!/bin/bash

# This scripts require CASSANDRA_VERSION, CASSANDRA_STRATIO_LUCENE_VERSION
# to be set.
# Example:
# export CASSANDRA_VERSION=3.11.1
# export CASSANDRA_STRATIO_LUCENE_VERSION=${CASSANDRA_VERSION}.0

CASSANDRA_HOME=$(pwd)"/.deps/apache-cassandra-${CASSANDRA_VERSION}/"

mkdir .deps

if [ -n "${CASSANDRA_VERSION}" ]; then
    cd .deps
    wget "http://www.us.apache.org/dist/cassandra/${CASSANDRA_VERSION}/apache-cassandra-${CASSANDRA_VERSION}-bin.tar.gz"
    tar -xzf "apache-cassandra-${CASSANDRA_VERSION}-bin.tar.gz"
    cd -
fi


if [ -n "${CASSANDRA_STRATIO_LUCENE_VERSION}" ]; then
    cd .deps
    git clone http://github.com/Stratio/cassandra-lucene-index
    cd cassandra-lucene-index
    git checkout ${CASSANDRA_STRATIO_LUCENE_VERSION}
    git checkout -b ${CASSANDRA_STRATIO_LUCENE_VERSIO}

    mvn clean package -DskipTests
    cp plugin/target/cassandra-lucene-index-plugin-*.jar ${CASSANDRA_HOME}/lib/
fi
