**THIS SOFTWARE IS NOT READY FOR PRODUCTION USE AND IS PROVIDED AS-IS**

[![Build Status](https://travis-ci.org/criteo/biggraphite.svg?branch=master)](https://travis-ci.org/criteo/biggraphite)
[![Coverage Status](https://coveralls.io/repos/github/criteo/biggraphite/badge.svg)](https://coveralls.io/github/criteo/biggraphite?branch=master)
[![Dependency Status](https://gemnasium.com/badges/github.com/criteo/biggraphite.svg)](https://gemnasium.com/github.com/criteo/biggraphite)
[![Join the chat at https://gitter.im/criteo/biggraphite](https://badges.gitter.im/criteo/biggraphite.svg)](https://gitter.im/criteo/biggraphite?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)


# Big Graphite

BigGraphite is a storage layer for timeseries data. It integrates with Graphite as a plugin.

For usage information and how to contribute, please see [CONTRIBUTING.md](CONTRIBUTING.md).

# Usage

See [USAGE.md](USAGE.md) and [CONFIGURATION.md](CONFIGURATION.MD).

# Contact

- [Gitter](https://gitter.im/criteo/biggraphite)
- [Mailing List](https://groups.google.com/forum/#!forum/biggraphite)


# Backends

There is only one supported backend for now: Cassandra, whose design is [described in CASSANDRA_DESIGN.md](CASSANDRA_DESIGN.md).


# Code structure

- `biggraphite.accessor` exposes the public API to store/retrieve metrics
- `biggraphite.metadata_cache` implements a machine-local cache using [LMDB](https://lmdb.readthedocs.io) so that one does not need a round-trip for each call to `accessor`
- `biggraphite.plugins.*` implements integration with Carbon and Graphite
- `biggraphite.backends.*` implements the storage backends (eg: Cassandra-specific code)
