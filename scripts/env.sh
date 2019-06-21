#!/bin/sh

# Provide base environment for tests and test scripts.
#
# Usage:
#  $ source env.sh
#

export PROJECT_ROOT=$(git rev-parse --show-toplevel)

cd ${PROJECT_ROOT}

export BG_VENV=bg

test -d "${BG_VENV}" && INITIALIZE_BG_VENV=false || INITIALIZE_BG_VENV=true

if ${INITIALIZE_BG_VENV}; then
    virtualenv ${BG_VENV}
fi

. ${BG_VENV}/bin/activate

# By default carbon and graphite-web are installed in /opt/graphite,
# We want NO prefix in order to have a good interaction with virtual env.
export GRAPHITE_NO_PREFIX=true

if ${INITIALIZE_BG_VENV}; then
    # Install Graphite Web and Carbon
    pip install graphite-web
    pip install carbon

    # Install the libffi-dev package from your distribution before running pip install
    pip install -r requirements.txt
    pip install -r tests-requirements.txt
    pip install -e .
fi

# In test environment, limit to 12 components
export BG_COMPONENTS_MAX_LEN=12

export CASSANDRA_VERSION=3.11.3
export CASSANDRA_STRATIO_LUCENE_VERSION=${CASSANDRA_VERSION}.0
export ES_VERSION=6.3.1

./tools/install-deps.sh

export CASSANDRA_HOME=$(pwd)"/.deps/apache-cassandra-${CASSANDRA_VERSION}/"
export ES_HOME=$(pwd)"/.deps/elasticsearch-${ES_VERSION}/"
export CASSANDRA_HOSTPORT=localhost:9042
export ES_HOSTPORT=localhost:9200
