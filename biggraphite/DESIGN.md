# BigGraphite Design

BigGraphite is a storage layer for timeseries data. It integrates as a plugin to Graphite.\
**This document describes general principles**, for a description of usage see USAGE.md.

## Code structure
- `biggraphite.accessor` exposes the public API to store/retrieve metrics
- `biggraphite.metadata_cache` implements a machine-local cache using [LMDB](https://lmdb.readthedocs.io)
   so that one does not need a round-trip for each call to `accessor`.
- `biggraphite.plugins.*` implements integration with Carbon and Graphite
- `biggraphite.backends.*` implements the storage backends (eg: Cassandra-specific code)

## Backends

There is only one supported backend for now: Cassandra.