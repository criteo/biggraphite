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

To set-up your test enviroment, source `scripts/env.sh`:

```bash
source scripts/env.sh
```

Cassandra tests generate a lot of I/O, so if you are planning to run tests you will also need to mount `/tmp` as tmpfs unless you have a fast SSD.
You can run this script to do it quickly:

```bash
./tools/mount-tmpfs.sh
```

It will mount the `data` directories (for Cassandra and Elasticsearch) into `/dev/shm`


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

### Running tests faster

By default, integration tests will start and stop a database instance for every test (Cassandra or
Elasticsearch). To make them run faster, you can run your own instance and provide the connection
information using environment variable.

Example:

```
ES_HOSTPORT=127.0.0.1:9200 CASSANDRA_HOSTPORT=127.0.0.1:9042 tox
```

To make the tests go even faster, move the `data` directories of Elasticsearch and Cassandra
to /dev/shm.

```
rm -rf .deps/elasticsearch-${ES_VERSION}/data
mkdir /dev/shm/es-bg-data
ln -s /dev/shm/es-bg-data .deps/elasticsearch-${ES_VERSION}/data
```

Scripts are provided to let you automate it easily:

```bash
$ source scripts/env.sh
Skip cassandra installation, already installed
Skip Cassandra Statio Lucene installation. Already installed
Skip elasticsearch installation. Already installed
$ ./tools/mount-tmpfs.sh
Mounting Cassandra data directory to tmpfs
Mounting Elasticsearch data directory to tmpfs
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

## Using Docker (New!)

https://hub.docker.com/_/cassandra/

docker run -it --link cassandra --rm cassandra sh -c 'exec cqlsh "$CASSANDRA_PORT_9042_TCP_ADDR"'