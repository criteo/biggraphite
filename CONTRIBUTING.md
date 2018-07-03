# How to contribute to BigGraphite

First of all, thank you! :heart: :heart: :heart:

This document explains how to contribute code or bug reports, but feel free to ask your questions on [Gitter](https://gitter.im/criteo/biggraphite) or our [mailing list](https://groups.google.com/forum/#!forum/biggraphite).


## Did you find a bug?

1. Please ensure the bug was not already reported by searching the [Github Issues](https://github.com/criteo/biggraphite/issues).
2. If you are unable to find an open issue addressing the problem, [open a new one](https://github.com/criteo/biggraphite/issues/new). Make sure to include a *title and clear description* and as much relevant information as possible.


## Code contributions

See [README](README.md) for the code structure.


### Style guide

By convention:
- This project follows [PEP 8](https://www.python.org/dev/peps/pep-0008/) and [PEP 257](https://www.python.org/dev/peps/pep-0257/)
- We indent by four space
- We only import modules (`from os import path as os_path` not `from os.path import join`)


### Did you write a patch that fixes a bug?

- Check information on style above, `pylama tests biggraphite` will tell you about many violations
- Create a new pull request on Github with the patch
- Ensure the pull request description is clear about the problem it solves and the suggested solution. If applicable, include the relevant issue number


### Do you intend to add a new feature or change an existing one?

- Check information on style above
- Suggest your change on the [mailing list](https://groups.google.com/forum/#!forum/biggraphite) and start writing code
- Do not open an issue until you have collected positive feedback about the change: issues are primarily intended for bug reports and fixes


### Test Environment

```bash
# Setup the virtualenv
export BG_VENV=bg
virtualenv ${BG_VENV}
source ${BG_VENV}/bin/activate

# By default carbon and graphite-web are installed in /opt/graphite,
# We want NO prefix in order to have a good interaction with virtual env.
export GRAPHITE_NO_PREFIX=true

# Install Graphite Web and Carbon
pip install graphite-web
pip install carbon

# Install the libffi-dev package from your distribution before running pip install
pip install -r requirements.txt
pip install -r tests-requirements.txt
pip install -e .

# Install Cassandra
export CASSANDRA_VERSION=3.11.2
wget "http://www.us.apache.org/dist/cassandra/${CASSANDRA_VERSION}/apache-cassandra-${CASSANDRA_VERSION}-bin.tar.gz"
tar -xzf "apache-cassandra-${CASSANDRA_VERSION}-bin.tar.gz"
export CASSANDRA_HOME=$(pwd)/apache-cassandra-${CASSANDRA_VERSION}
```

Cassandra tests generate a lot of I/O, so if you are planning to run tests you will also need to mount `/tmp` as tmpfs unless you have a fast SSD.


### Running tests

You can use `tox` to run tests. You will need dev packages from your distribution for each version of Python used by Tox: python-dev, pypy-dev.

```bash
$ pip install tox
$ tox
```

You can also simply use `unittest.discover` if you have a working dev environment (see previous paragraph).

```bash
$ BG_COMPONENTS_MAX_LEN=12 python -m unittest discover --failfast --verbose --catch
```

### Running Benchmarks

The simpliest way is to use tox. The command below will run all the tests inside benchmarks/ directory.
```bash
tox -e bench
```

To be able to compare your new changes, First run the command below before you make any changes.
It will run all the benchmarks and save the result for later analyze.
```bash
tox -e bench -- --benchmark-autosave
```

Now after any changes, you can run the commmand to see if your results improves things or not
```bash
tox -e bench -- --benchmark-compare
```

If you want to run the benchmarks manually, it uses [pytest-benchmark](https://pypi.python.org/pypi/pytest-benchmark)
```bash
pytest path_to_your_benchmark
```


### Test instance

Assuming you have a working development environment, here is how to run a test instance of Graphite Web reading metrics from Cassandra.


#### Cassandra

If you do not have a test Cassandra cluster around, you can use [CCM](https://github.com/pcmanus/ccm) to setup a local one.

The following will start Cassandra (if you have a manual setup) and import the base schema.
Beware that the current schema uses SimpleStrategy with 1 replica: in production you should increase the replication factor.

```bash
$ ${CASSANDRA_HOME}/bin/cassandra
$ ${CASSANDRA_HOME}/bin/cqlsh < share/schema.cql
$ export BG_COMPONENTS_MAX_LEN=12
$ bgutil syncdb
```


#### Carbon

Change (or append) the following settings to the `carbon.conf` file:

```text
[cache]
BG_CASSANDRA_KEYSPACE = biggraphite
BG_CASSANDRA_CONTACT_POINTS = 127.0.0.1
BG_DRIVER = cassandra
BG_CACHE = memory
DATABASE = biggraphite
STORAGE_DIR = /tmp
```

You can test your new configuration with:

```bash
touch storage-schemas.conf
bg-carbon-cache --debug --nodaemon --conf=carbon.conf start
echo "local.random.diceroll 4 `date +%s`" | nc -q0 localhost 2003
```


#### Graphite Web

Edit `${BG_VENV}/lib/python2.7/site-packages/graphite/local_settings.py` and add:

```python
import os
DEBUG = True
LOG_DIR = '/tmp'
STORAGE_DIR = '/tmp'
STORAGE_FINDERS = ['biggraphite.plugins.graphite.Finder']
TAGDB = 'biggraphite.plugins.tags.BigGraphiteTagDB'
BG_CASSANDRA_KEYSPACE = 'biggraphite'
BG_CASSANDRA_CONTACT_POINTS = '127.0.0.1'
BG_DRIVER = 'cassandra'
BG_CACHE = 'memory'
WEBAPP_DIR = "%s/webapp/" % os.environ['BG_VENV']
```

You can now start Graphite Web:

```bash
export DJANGO_SETTINGS_MODULE=graphite.settings
django-admin migrate
django-admin migrate --run-syncdb
run-graphite-devel-server.py ${BG_VENV}
```
