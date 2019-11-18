#!/bin/bash
export DJANGO_SETTINGS_MODULE=graphite.settings
django-admin migrate
django-admin migrate --run-syncdb
cd /usr/local/lib/python3.6/site-packages/graphite && run-graphite-devel-server.py /usr/local
