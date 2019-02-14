#!/bin/bash

# This scripts require CASSANDRA_VERSION, CASSANDRA_STRATIO_LUCENE_VERSION,
# ES_VERSION to be set.
# Example:
# export CASSANDRA_VERSION=3.11.3
# export CASSANDRA_STRATIO_LUCENE_VERSION=${CASSANDRA_VERSION}.0
# export ES_VERSION=6.3.1

CASSANDRA_HOME=$(pwd)"/.deps/apache-cassandra-${CASSANDRA_VERSION}/"
ES_HOME=$(pwd)"/.deps/elasticsearch-${ES_VERSION}/"

function install_cassandra()
{
    if [ ! -d "${CASSANDRA_HOME}" ]; then
        echo "Installing Cassandra"
        cd .deps
        FILENAME="apache-cassandra-${CASSANDRA_VERSION}-bin.tar.gz"
        if [ ! -f "${FILENAME}" ]; then
            wget "http://archive.apache.org/dist/cassandra/${CASSANDRA_VERSION}/${FILENAME}"
            tar -xzf "${FILENAME}"
        fi
        cd -
    else
        echo "Skip cassandra installation, already installed in ${CASSANDRA_HOME}"
    fi
}

function install_lucene()
{
    if ! ls ${CASSANDRA_HOME}/lib/cassandra-lucene-index-plugin-*.jar 1> /dev/null 2>&1; then
        if [ -n "${CASSANDRA_STRATIO_LUCENE_VERSION}" ]; then
            echo "Installing Cassandra Statio Lucene plugin"
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
        else
            echo "Skip Cassandra Statio Lucene installation. CASSANDRA_STRATIO_LUCENE_VERSION is not set"
        fi
    else
        echo "Skip Cassandra Statio Lucene installation. Already installed"
    fi

}
function install_cassandra_with_lucene()
{
    if [ -n "${CASSANDRA_VERSION}" ]; then
        install_cassandra
        install_lucene
    else
        echo "Skip cassandra installation. CASSANDRA_VERSION is not set"
    fi
}

function install_elasticsearch()
{
    if [ ! -d "${ES_HOME}" ]; then
        if [ -n "${ES_VERSION}" ]; then
            echo "Installing Elasticsearch"
            cd .deps
            FILENAME="elasticsearch-${ES_VERSION}.tar.gz"
            if [ ! -f "${FILENAME}" ]; then
                wget "https://artifacts.elastic.co/downloads/elasticsearch/${FILENAME}"
                tar -xzf "${FILENAME}"
            fi
            cd -
        else
            echo "Skip elasticsearch installation. ES_VERSION is not set"
        fi
    else
        echo "Skip elasticsearch installation. Already installed in ${ES_HOME}"
    fi
}

mkdir -p .deps
install_cassandra_with_lucene
install_elasticsearch
