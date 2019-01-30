#! /bin/bash

BGH_CONF=volumes_bg-helper/conf
AGG_CONF=volumes_graphite-aggregator-cache/conf
WEB_CONF=volumes_graphite-web/conf

build_bg-cassandra (){
  docker build bg-cassandra -t bg-cassandra -f bg-cassandra/Dockerfile
}

build_bg-helper (){
  docker build bg-helper -t bg-helper -f bg-helper/Dockerfile

  mkdir -p ${BGH_CONF}

  cat << EOF > ${BGH_CONF}/bg-cassandra.sh
#! /bin/bash

while [ \$(cqlsh -e "USE biggraphite" cassandra --cqlversion="3.4.4" || echo 1) ]; do sleep 0.5; cqlsh -e "CREATE KEYSPACE IF NOT EXISTS biggraphite WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'} AND durable_writes = false;" cassandra --cqlversion="3.4.4"; done \
&& echo "biggraphite keyspace created" \
&& cqlsh -e "DESCRIBE biggraphite" cassandra --cqlversion="3.4.4"
EOF
}

build_bg-elasticsearch (){
  docker build bg-elasticsearch -t bg-elasticsearch -f bg-elasticsearch/Dockerfile
}

build_bg-kibana (){
  docker build bg-kibana -t bg-kibana -f bg-kibana/Dockerfile
}

build_graphite-aggregator-cache (){
  docker build .. -t graphite-aggregator-cache -f graphite-aggregator-cache/Dockerfile

  mkdir -p ${AGG_CONF}

  cat << EOF > ${AGG_CONF}/carbon.conf
[cache]
BG_DATA_DRIVER = cassandra
BG_METADATA_DRIVER = elasticsearch
DATABASE = biggraphite
BG_CASSANDRA_KEYSPACE = biggraphite
BG_CASSANDRA_CONTACT_POINTS = cassandra
BG_CASSANDRA_CONTACT_POINTS_METADATA =
BG_CACHE = memory
STORAGE_DIR = /tmp
BG_ELASTICSEARCH_HOSTS = elasticsearch
BG_ELASTICSEARCH_PORT = 9200
#BG_ELASTICSEARCH_INDEX_SUFFIX = _%Y-w%W
EOF

  touch ${AGG_CONF}/storage-schemas.conf
}


build_graphite-web (){
  docker build .. -t graphite-web -f graphite-web/Dockerfile

  mkdir -p ${WEB_CONF}

  cat << EOF > ${WEB_CONF}/local_settings.py
DEBUG = True
LOG_DIR = '/tmp'
STORAGE_DIR = '/tmp'
STORAGE_FINDERS = ['biggraphite.plugins.graphite.Finder']
TAGDB = 'biggraphite.plugins.tags.BigGraphiteTagDB'
# Cassandra configuration
BG_CASSANDRA_KEYSPACE = 'biggraphite'
BG_CASSANDRA_CONTACT_POINTS = 'cassandra'
BG_DATA_DRIVER = 'cassandra'
BG_CACHE = 'memory'
WEBAPP_DIR = '/usr/local/webapp/'
## Elasticsearch configuration
BG_METADATA_DRIVER = 'elasticsearch'
BG_ELASTICSEARCH_HOSTS = 'elasticsearch'
BG_ELASTICSEARCH_PORT = '9200'
EOF

  cat << EOF > ${WEB_CONF}/launch-graphite-web.sh
#! /bin/bash

cp /conf/local_settings.py /usr/local/lib/python3.6/site-packages/graphite/
export DJANGO_SETTINGS_MODULE=graphite.settings
django-admin migrate
django-admin migrate --run-syncdb
cd /usr/local/lib/python3.6/site-packages/graphite && run-graphite-devel-server.py /usr/local
EOF
}

build (){
  case ${1} in
    bg-cassandra)
      build_bg-cassandra ;;
    bg-helper)
      build_bg-helper ;;
    bg-elasticsearch)
      build_bg-elasticsearch ;;
    bg-kibana)
      build_bg-kibana ;;
    graphite-aggregator-cache)
      build_graphite-aggregator-cache ;;
    graphite-web)
      build_graphite-web ;;
    *)
      usage ;;
  esac
}

usage (){
  echo "usage: $(basename ${0}) [dir1_containing_DockerFile [dir2_containing_DockerFile ..]]"
  echo "with no argument it builds all image defined in directories containing a DockerFile"
  exit 1
}


if [ -z "${1}" ]; then
  for i in $(ls */Dockerfile | sed 's/\(.*\)\/.*/\1/');
  do
    TMP="${TMP} ${i}"
  done
else
  while (( "$#" )); do
    TMP="${TMP} ${1%/}"
    shift
  done
fi

# Remove lead whitespace
LIST=$(echo ${TMP} | awk '$1=$1')

for i in ${LIST}; do
  build ${i}
done
