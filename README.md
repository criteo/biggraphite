[![Build Status](https://travis-ci.org/criteo/biggraphite.svg?branch=master)](https://travis-ci.org/criteo/biggraphite)
[![Coverage Status](https://coveralls.io/repos/github/criteo/biggraphite/badge.svg?branch=initialimport)](https://coveralls.io/github/criteo/biggraphite?branch=master)
[![Dependency Status](https://gemnasium.com/badges/github.com/criteo/biggraphite.svg)](https://gemnasium.com/github.com/criteo/biggraphite)

Big Graphite
============

[![Join the chat at https://gitter.im/criteo/biggraphite](https://badges.gitter.im/criteo/biggraphite.svg)](https://gitter.im/criteo/biggraphite?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

We are experimenting with storing Graphite metrics in Cassandra. This repository contains related code.

*None of it is ready for its premiere yet.*


Developpment
============

Environment
-----------

```bash
# Setup the virtualenv
$ export BG_VENV=bg
$ virtualenv ${BG_VENV}
$ source bg/bin/activate

# Install Graphite dependencies
$ export GRAPHITE_NO_PREFIX=true
$ pip install -r requirements.txt
$ pip install -r tests-requirements.txt

# Install Cassandra
$ export CASSANDRA_VERSION=3.5
$ wget "http://www.us.apache.org/dist/cassandra/${CASSANDRA_VERSION}/apache-cassandra-${CASSANDRA_VERSION}-bin.tar.gz"
$ tar -xzf "apache-cassandra-${CASSANDRA_VERSION}-bin.tar.gz"
$ export CASSANDRA_HOME=$(pwd)/apache-cassandra-${CASSANDRA_VERSION}
```

If you're planning to run tests, you'll also need to mount /tmp as tmpfs unless
you have a fast SSD.

Tests
-----

To run tests you can either use `tox`:

```bash
$ pip install tox
$ tox
```

You can also simply use unittest.discover if you have a working dev environment.


```bash
$ python -m unittest discover --failfast --verbose --catch
```

Test instance
-------------

Assuming you have a working dev environment, here is how to run a test instance
of Graphite Web reading metrics from Cassandra:

```bash
$ pip install -e .
$ export DJANGO_SETTINGS_MODULE=graphite.settings
$ django-admin migrate
$ django-admin migrate --run-syncdb
```

Edit ${BG_VENV}/lib/python2.7/site-packages/graphite/local_settings.py and put

```python
import os
DEBUG = True
LOG_DIR = '/tmp'
STORAGE_DIR = '/tmp'
STORAGE_FINDERS = ['biggraphite.plugins.graphite.Finder']
BG_KEYSPACE = 'biggraphite'
BG_CONTACT_POINTS = '<MY_CASSANDRA_NODES>'
WEBAPP_DIR = "%s/webapp/" % os.environ['BG_VENV']
```

Start Graphite Web

```bash
$ run-graphite-devel-server.py ${BG_VENV}
```

Contact
=======

Mailing list: https://groups.google.com/forum/#!forum/biggraphite
