# BigGraphite

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).

## [Unreleased]

## [0.14.10] - 2019-02-19

### Improved

- Wait metadata to be read before touching them

## [0.14.9] - 2019-02-14

### Improved

- Pass correct time argument to carbonlink merge function
- Adding docker-compose setup to test biggraphite

### Fixed

- Fixed accessor cache on python 3.x
- Fixed bug in ES metrics_matching_glob
- Fixed data read/write consistency level in cassandra driver

## [0.14.8] - 2019-01-04

### Fixed

- crash when opencensus is not installed

## [0.14.7] - 2018-12-27

### Improved

- Tracing shows more relevant data in spans (metric names/globs if available)
- Middleware is provided to improve tracing:
  - more options to configure when to trace
  - more data in spans (the executed query and the format)

## [0.14.6] - 2018-12-18

### Improved

- reduced number of metadata objects handled by biggraphite

### Fixed

- some bgutils commands were printing python objects instead of string representation

## [0.14.5] - 2018-12-13

### Fixed

- adapt biggraphite to use the lastest version of prometheus client

## [0.14.4] - 2018-12-13

### Fixed

- monitoring: some Cassandra counters were stuck at 0
- Cassandra backend returned empty results in traced environment
- specifing the same data driver as metadata driver lead to runtime errors
- diskcache not used with Cassandra

## [0.14.3] - 2018-11-26

### Fixed

- fix list index out of range in _get_metrics when the metric doesn't exists

### Added

- support tracing of biggraphite
- expose metrics from the elasticsearch driver

## [0.14.2] - 2018-10-18

### Fixed

- CVE-2017-12629 on graphiteindex (upgrade to Lucene 7.1.0)

## [0.14.1] - 2018-10-15

### Improved

- improved observability on metric creation

## [0.14.0]

### Improved

- performance of Elasticsearch driver

### Fixed

- isolation between metadata and data in Cassandra driver
- metric name UTF-8 support in Elasticsearch driver

### Added

- ability to disable BigGraphite cache

### Removed

- irrelevant metrics on `bg_cassandra_updated_on_latency_seconds` and
  `bg_cassandra_read_on_latency_seconds` for Cassandra driver

## [0.13.10] - 2018-10-08

### Improved

- cassandra accessor observability: add metrics on query usage

## [0.13.9] - 2018-08-28

### Fixed

- updated_on behavior

### Improved

- bgutil stats supports metric prefix

## [0.13.8] - 2018-08-27

### Fixed

- cassandra: don't loose points if we have many restarts
- Fix python 3.6 compatibility

### Improved

- bgutil stats: add carbon support
- Add flask_cors support

## [0.13.7] - 2018-08-23

### Fixed

- elasticsearch: env variables are now working properly

### Improved

- elasticsearch: timestamps are now updated on a background thread

## [0.13.6] - 2018-08-22

### Added

- elasticsearch: fix read_on bug
- bgutil web: add /maintainance page

## [0.13.5] - 2018-08-21

### Added

- Support clean/stats/repair in ES
- bgutil web: fix and finish async commands

## [0.13.4] - 2018-08-16

### Fixed

- Don't install ipython as requirement.

## [0.13.3] - 2018-08-14

### Added

- Configurable es schema
- bgutil web: async cmd
- bgutil web: capture stderr/stdout

### Fixed

- ES import script (schema + doc + remove directory + doc type)
- Wrong C* metric metadata parsing

## [0.13.2] - 2018-08-03

### Removed

- Configurable es schema (reverted while waiting for a fix)
- Accessor clear method (only used by tests)

## [0.13.1] - 2018-08-02

### Added

- Use provided time range to glob search metrics and directories (Elasticsearch only)

### Fixed

- Metadata update lost creation date in Elasticsearch backend
- Ignore update conflicts on Elasticsearch (HTTP 409)

## [0.13.0] - 2018-23-07

### Added

- Elasticsearch driver (supports metadata only), see [ELASTICSEARCH_DESIGN.md](ELASTICSEARCH_DESIGN.md)
- Ability to declare distinct data and metadata drivers: use `--data_driver` and `--metadata-driver` instead of
  `--driver` (`--driver` option is still supported)

## [0.12.0] - 2018-25-06

### Added

- bgutil clean new flag: --corrupted-metrics
- Add non-zero timeout to event.wait()
- Make sure we always create missing directories

### Fixed

- Non escaped char in the Lucene filter string
- Fix disk cache and add unitests

## [0.11.0] - 2018-03-13

### Added

- Metrics for metadata cache and cassandra operations.
- Ability to configure SYNC_EVERY_N_WRITES in carbon
- Shuffle Cassandra replicas

### Fixed

- Connection leak when Cassandra connection fails

## [0.10.7] - 2018-02-15

### Added

- Added bgutil graphite_web to debug the graphite web plugin.

### Fixed

- Reduce calls to biggraphite_metadata.metrics_metadata when reading points.

## [0.10.6] - 2018-02-15

### Added

- New options for bg-replay-traffic

### Fixed

- metrics_metadata is now less subject to corruptions

## [0.10.5] - 2018-02-01

### Fixed

- Cache issues with globs

## [0.10.4] - 2018-01-31

### Added

- Experimental Lucene index (with BG_CASSANDRA_USE_LUCENE).

## [0.10.3] - 2018-01-15

### Fixed

- Fixed wrong results when using performance improvements for 1.1.x
- Improved the way readers and finders were imported.

## [0.10.2] - 2018-01-08

### Fixed

- Fixed performance regression with 1.1.0
- Fixed some bugs caused by the Python 3 support

## [0.10.0] - 2017-12-20

### New

- Support for Graphite 1.1.0
- Support for Python 3
- Add CSV support to bgutil read

### Fixed

- Bug where leaves_only would be ignored when caching values
- Queries returning no results would no be cached correctly
- Add locking in metadata_cache
- Fixed benchmarks

## [0.9.2] - 2017-12-04

### Fixed

- Fix compatibility with Graphite 1.1.0

## [0.9.1] - 2017-12-04

### Fixed

- Version in setup.py

### New

- Experiments with Lucence and ES

## [0.9.0] - 2017-11-21
### Breaking changes

- The previously non working `--(meta|data)_consistency*` flags have been prefixed by `cassandra`.

### Fixed

- Cassandra background operations (clean, repair) are now more resilient to timeouts
- Fixed Cassandra consistency flags.

## [0.8.11] - 2017-11-08

### Fixed

- `bgutil stats`: Skip corrupt metrics
- `bgutil write`: fix command and add unit test
- tools: Fix columnspec for time_start_ms.

### New

- `bgutil syncdb`: add a way to create datapoint tables

## [0.8.10] - 2017-10-12

### Breaking Change

- Renamed `created_at` -> `created_on`:
  Before updating, run:

  ```cql
  ALTER TABLE biggraphite_metadata.metrics_metadata ADD created_on timeuuid;
  CREATE CUSTOM INDEX metrics_metadata_created_on_idx ON biggraphite_metadata.metrics_metadata (created_on) USING 'org.apache.cassandra.index.sasi.SASIIndex' WITH OPTIONS = {'mode': 'SPARSE'};
  ```

  After updating, run:

  ```cql
  DROP CUSTOM INDEX metrics_metadata_created_at_idx;
  ALTER TABLE biggraphite_metadata.metrics_metadata DROP created_at;
  ```

### Fixed

- Carbon: make sure we touch metrics on all code path

### New

- `bgutil delete` to delete metrics

## [0.8.9] - 2017-09-26

### Fixed

- Only update metadata timestamps when writing

### New

- New created_at field in metrics_metadata
- Some work for the new GraphiteIndex, not ready yet.

## [0.8.8] - 2017-07-11

### Fixed

- Bug in cleanup code (would remove intermediate directories)

## [0.8.7] - 2017-07-07

### Fixed

- Infinite caching for metadata.

## [0.8.6] - 2017-06-15

### Fixed

- Infinite caching for metadata.

## [0.8.5] - 2017-06-15

### Fixed

- Fixed the time info of empty timeseries
- Fix error handling in repair()/clean()

### New

- Added bgutil stats
- Better caching for find()
- Use new Graphite-Web API for FindQuery
- Make read_on/updated_on more configurable

## [0.8.4] - 2017-05-15

### Fixed

- Better logging

### New

- Add `bgutil copy`
- Better exception in carbon.py when metric can't be found

## [0.8.3] - 2017-04-25

- Nothing, just an issue with pypi uploads.

## [0.8.2] - 2017-04-25

### Fixed

- Fix ipython and cassandra versions

## [0.8.1] - 2017-04-05

### Fixed

- Better handling of Cassandra exceptions
- Better handling of carbonlink
- Fix metric reporting interval

## [0.8.0] - 2017-03-10

### Added

- Configurable consistency settings (#242)
- [BREAKING] New column (read_on) in metric_metadata for adding statistics on read metrics (#107)
  need to run the following cql to migrate theschema

```SQL
   ALTER TABLE biggraphite_metadata.metrics_metadata
   ADD read_on timeuuid;
```

- Added a tool to replay graphite traffic
- Support Carbon Link protocol

### Fixed

- Fix multi-cluster support
- Be more robust when metadata is missing

### Changed

- Added some randomness to cache expiration

## [0.7.0]

### Added

- Prometheus support and additional metrics
- Add support for multiple replicas/writers
- Microbenchmarks
- Asynchronous creation of metrics in carbon
- Better bg-import-whisper / bg-generate-sstables + helper scripts

### Fixed

- Multiple bugs with the disk cache
- Multiple clean/repair bugs

### Changed

- [Breaking] Cassandra schema as been changed to separate stage0 and aggregated
  metrics. There is no upgrade procedure as this is a pre-release.
- [Breaking] `bgutil syncdb` needs to be run during initial install

## [0.6.0] - 2016-11-25

### Added

- Initial release with this CHANGELOG file

### Changed

- We are going to do releases from now on

[Unreleased]: https://github.com/criteo/biggraphite/compare/v0.14.10...HEAD
[0.14.10]: https://github.com/criteo/biggraphite/compare/v0.14.9...v0.14.10
[0.14.9]: https://github.com/criteo/biggraphite/compare/v0.14.8...v0.14.9
[0.14.8]: https://github.com/criteo/biggraphite/compare/v0.14.7...v0.14.8
[0.14.7]: https://github.com/criteo/biggraphite/compare/v0.14.6...v0.14.7
[0.14.6]: https://github.com/criteo/biggraphite/compare/v0.14.5...v0.14.6
[0.14.5]: https://github.com/criteo/biggraphite/compare/v0.14.4...v0.14.5
[0.14.4]: https://github.com/criteo/biggraphite/compare/v0.14.3...v0.14.4
[0.14.3]: https://github.com/criteo/biggraphite/compare/v0.14.2...0.14.3
[0.14.2]: https://github.com/criteo/biggraphite/compare/v0.14.1...v0.14.2
[0.14.1]: https://github.com/criteo/biggraphite/compare/v0.14.0...v0.14.1
[0.14.0]: https://github.com/criteo/biggraphite/compare/v0.13.10...v0.14.0
[0.13.10]: https://github.com/criteo/biggraphite/compare/v0.13.9...v0.13.10
[0.13.9]: https://github.com/criteo/biggraphite/compare/v0.13.8...v0.13.9
[0.13.8]: https://github.com/criteo/biggraphite/compare/v0.13.7...v0.13.8
[0.13.7]: https://github.com/criteo/biggraphite/compare/v0.13.6...v0.13.7
[0.13.6]: https://github.com/criteo/biggraphite/compare/v0.13.5...v0.13.6
[0.13.5]: https://github.com/criteo/biggraphite/compare/v0.13.4...v0.13.5
[0.13.4]: https://github.com/criteo/biggraphite/compare/v0.13.3...v0.13.4
[0.13.3]: https://github.com/criteo/biggraphite/compare/v0.13.2...v0.13.3
[0.13.2]: https://github.com/criteo/biggraphite/compare/v0.13.1...v0.13.2
[0.13.1]: https://github.com/criteo/biggraphite/compare/v0.13.0...v0.13.1
[0.13.0]: https://github.com/criteo/biggraphite/compare/v0.12.0...v0.13.0
[0.12.0]: https://github.com/criteo/biggraphite/compare/v0.11.0...v0.12.0
[0.11.0]: https://github.com/criteo/biggraphite/compare/v0.10.7...v0.11.0
[0.10.7]: https://github.com/criteo/biggraphite/compare/v0.10.6...v0.10.7
[0.10.6]: https://github.com/criteo/biggraphite/compare/v0.10.5...v0.10.6
[0.10.5]: https://github.com/criteo/biggraphite/compare/v0.10.4...v0.10.5
[0.10.4]: https://github.com/criteo/biggraphite/compare/v0.10.3...v0.10.4
[0.10.3]: https://github.com/criteo/biggraphite/compare/v0.10.2...v0.10.3
[0.10.2]: https://github.com/criteo/biggraphite/compare/v0.10.1...v0.10.2
[0.10.1]: https://github.com/criteo/biggraphite/compare/v0.10.0...v0.10.1
[0.10.0]: https://github.com/criteo/biggraphite/compare/v0.9.2...v0.10.0
[0.9.2]: https://github.com/criteo/biggraphite/compare/v0.9.1...v0.9.2
[0.9.1]: https://github.com/criteo/biggraphite/compare/v0.9.0...v0.9.1
[0.9.0]: https://github.com/criteo/biggraphite/compare/v0.8.11...v0.9.0
[0.8.11]: https://github.com/criteo/biggraphite/compare/v0.8.10...v0.8.11
[0.8.10]: https://github.com/criteo/biggraphite/compare/v0.8.9...v0.8.10
[0.8.9]: https://github.com/criteo/biggraphite/compare/v0.8.8...v0.8.9
[0.8.8]: https://github.com/criteo/biggraphite/compare/v0.8.7...v0.8.8
[0.8.7]: https://github.com/criteo/biggraphite/compare/v0.8.6...v0.8.7
[0.8.6]: https://github.com/criteo/biggraphite/compare/v0.8.5...v0.8.6
[0.8.5]: https://github.com/criteo/biggraphite/compare/v0.8.4...v0.8.5
[0.8.4]: https://github.com/criteo/biggraphite/compare/v0.8.3...v0.8.4
[0.8.3]: https://github.com/criteo/biggraphite/compare/v0.8.2...v0.8.3
[0.8.2]: https://github.com/criteo/biggraphite/compare/v0.8.1...v0.8.2
[0.8.1]: https://github.com/criteo/biggraphite/compare/v0.8.0...v0.8.1
[0.8.0]: https://github.com/criteo/biggraphite/compare/v0.7.0...v0.8.0
[0.7.0]: https://github.com/criteo/biggraphite/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/criteo/biggraphite/compare/v0.6.0
