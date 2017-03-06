# BigGraphite
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).

## [Unreleased]
### Added
- Configurable consistency settings (#242)
- [BREAKING] New column (read_on) in metric_metadata for adding statistics on read metrics (#107)
  need to run the following cql to migrate theschema
```SQL
   ALTER TABLE biggraphite_metadata.metrics_metadata
   ADD read_on timeuuid;
```

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


[Unreleased]: https://github.com/criteo/biggraphite/compare/v0.7.0...HEAD
[0.7]: https://github.com/criteo/biggraphite/compare/v0.7.0
[0.6]: https://github.com/criteo/biggraphite/compare/v0.6.0

