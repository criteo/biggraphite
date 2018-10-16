#/bin/sh

source /opt/venv/bin/activate

export DJANGO_SETTINGS_MODULE=graphite.settings
django-admin migrate
django-admin migrate --run-syncdb
bgutil syncdb

bg-carbon-cache --debug --nodaemon --conf=carbon.conf start &
run-graphite-devel-server.py /opt/venv/webapp/ &

bgutil web
