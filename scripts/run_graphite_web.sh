#!/bin/sh

cd $(git rev-parse --show-toplevel)
source scripts/env.sh

SITE_PACKAGES=$(find ${BG_VENV} -name "site-packages")
GRAPHITE_WEB_CONF="${SITE_PACKAGES}/graphite/local_settings.py"

if [ ! -f "${GRAPHITE_WEB_CONF}" ]; then
echo "No Graphite Web configuration, creating default one"
tee -a ${GRAPHITE_WEB_CONF} << END
import os
DEBUG = True
LOG_DIR = '/tmp'
STORAGE_DIR = '/tmp'
STORAGE_FINDERS = ['biggraphite.plugins.graphite.Finder']
TAGDB = 'biggraphite.plugins.tags.BigGraphiteTagDB'

# Cassandra configuration
BG_CASSANDRA_KEYSPACE = 'biggraphite'
BG_CASSANDRA_CONTACT_POINTS = '127.0.0.1'
BG_DRIVER = 'cassandra'
BG_CACHE = 'memory'
WEBAPP_DIR = "%s/webapp/" % os.environ['BG_VENV']

## Elasticsearch configuration
# BG_DRIVER = 'elasticsearch'
# BG_CACHE = 'memory'
# WEBAPP_DIR = "%s/webapp/" % os.environ['BG_VENV']
#
# BG_ELASTICSEARCH_HOSTS = 'elasticsearch.fqdn'
# BG_ELASTICSEARCH_PORT = 'elasticsearch.port'
# BG_ELASTICSEARCH_USERNAME = 'foo'
# BG_ELASTICSEARCH_PASSWORD = 'bar'
END
fi

export DJANGO_SETTINGS_MODULE=graphite.settings
django-admin migrate
django-admin migrate --run-syncdb
run-graphite-devel-server.py ${BG_VENV}