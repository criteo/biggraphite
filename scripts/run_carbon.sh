#!/bin/sh

# Run Carbon backend for test purpose.
# This script does not ensure that dependencies are started. You should
# start yourself Elasticsearch or Cassandra if you are using them.
#
# Usage:
#  $ ./run_carbon.sh
#

cd $(git rev-parse --show-toplevel)
source scripts/env.sh

cd ${BG_VENV}/conf

if [ ! -f carbon.conf ]; then
echo "No carbon configuration, creating default one"
tee -a carbon.conf << END
[cache]
# Cassandra configuration
BG_CASSANDRA_KEYSPACE = biggraphite
BG_CASSANDRA_CONTACT_POINTS = 127.0.0.1
BG_DRIVER = cassandra
BG_CACHE = memory
DATABASE = biggraphite
STORAGE_DIR = /tmp

## Elasticsearch configuration
# BG_DRIVER = elasticsearch
# BG_CACHE = memory
# DATABASE = biggraphite
# STORAGE_DIR = /tmp
# BG_ELASTICSEARCH_HOSTS = elasticsearch.fqdn
# BG_ELASTICSEARCH_PORT = 8080
# BG_ELASTICSEARCH_USERNAME = foo
# BG_ELASTICSEARCH_PASSWORD = bar
END
fi

touch storage-schemas.conf
bg-carbon-cache --debug --nodaemon --conf=carbon.conf start
