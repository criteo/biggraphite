#!/bin/bash

# This scripts require CASSANDRA_VERSION, CASSANDRA_STRATIO_LUCENE_VERSION,
# ES_VERSION to be set.
# Example:
# export CASSANDRA_VERSION=3.11.2
# export CASSANDRA_STRATIO_LUCENE_VERSION=${CASSANDRA_VERSION}.0
# export ES_VERSION=6.3.1

CASSANDRA_HOME=$(pwd)"/.deps/apache-cassandra-${CASSANDRA_VERSION}/"
ES_HOME=$(pwd)"/.deps/elasticsearch-${ES_VERSION}/"

mkdir -p .deps

if [ ! -d "${CASSANDRA_HOME}" ]; then
    echo "Installing Cassandra"
    if [ -n "${CASSANDRA_VERSION}" ]; then
        cd .deps
        FILENAME="apache-cassandra-${CASSANDRA_VERSION}-bin.tar.gz"
        if [ ! -f "${FILENAME}" ]; then
            wget "http://www.us.apache.org/dist/cassandra/${CASSANDRA_VERSION}/${FILENAME}"
            tar -xzf "${FILENAME}"
        fi
        cd -
    fi
fi

if ! ls ${CASSANDRA_HOME}/lib/cassandra-lucene-index-plugin-*.jar 1> /dev/null 2>&1; then
    echo "Installing Cassandra Statio Lucene plugin"
    if [ -n "${CASSANDRA_STRATIO_LUCENE_VERSION}" ]; then
        if [ -n "${CASSANDRA_STRATIO_LUCENE_BUILD}" ]; then
            cd .deps
            git clone http://github.com/Stratio/cassandra-lucene-index
            cd cassandra-lucene-index
            git checkout ${CASSANDRA_STRATIO_LUCENE_VERSION}
            git checkout -b ${CASSANDRA_STRATIO_LUCENE_VERSION}

            mvn clean package -DskipTests
            cp -v plugin/target/cassandra-lucene-index-plugin-*.jar ${CASSANDRA_HOME}/lib/
        else
            # Download pre-built binaries
            wget --content-disposition https://search.maven.org/remotecontent?filepath=com/stratio/cassandra/cassandra-lucene-index-plugin/${CASSANDRA_STRATIO_LUCENE_VERSION}/cassandra-lucene-index-plugin-${CASSANDRA_STRATIO_LUCENE_VERSION}.jar
            cp -v cassandra-lucene-index-plugin-*.jar ${CASSANDRA_HOME}/lib/
        fi
    fi
fi

if [ ! -d "${ES_HOME}" ]; then
    echo "Installing Elasticsearch"
    if [ -n "${ES_VERSION}" ]; then
        cd .deps
        FILENAME="elasticsearch-${ES_VERSION}.tar.gz"
        if [ ! -f "${FILENAME}" ]; then
            wget "https://artifacts.elastic.co/downloads/elasticsearch/${FILENAME}"
            tar -xzf "${FILENAME}"
        fi
        cd -
    fi
fi
