#!/usr/bin/env python
# Copyright 2016 Criteo
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Abstracts querying Cassandra to manipulate timeseries."""
from __future__ import absolute_import
from __future__ import print_function

import logging

import cassandra
from cassandra import cluster as c_cluster
from cassandra import concurrent as c_concurrent
from cassandra import encoder as c_encoder
from cassandra import query as c_query
from cassandra.io import asyncorereactor as c_asyncorereactor

from biggraphite import accessor as bg_accessor
from biggraphite.drivers import _downsampling
from biggraphite.drivers import _utils


class Error(bg_accessor.Error):
    """Base class for all exceptions from this module."""


class CassandraError(Error):
    """Fatal errors accessing Cassandra."""


class RetryableCassandraError(CassandraError, bg_accessor.RetryableError):
    """Errors accessing Cassandra that could succeed if retried."""


class NotConnectedError(CassandraError):
    """Fatal errors accessing Cassandra because the Accessor is not connected."""


class TooManyMetrics(CassandraError):
    """A name glob yielded more than Accessor.MAX_METRIC_PER_GLOB metrics."""


class InvalidArgumentError(Error, bg_accessor.InvalidArgumentError):
    """Callee did not follow requirements on the arguments."""


class InvalidGlobError(InvalidArgumentError):
    """The provided glob is invalid."""

# HEURISTIC PARAMETERS
# ====================
# The following few constants are heuristics that are used to tune
# the datatable.
# TODO: Consider switching to TWCS after Cassandra 3.8
# We expect timestamp T to be written at T +/- _OUT_OF_ORDER_S
# As result we delay expiry and compaction by that much time
_OUT_OF_ORDER_S = 15 * 60
# We expect to use the last 1296 points (~24h with a resolution of one minute).
# We round it up to a power of min_threshold=4.
# As 1296=6^4, we will be compacting up to 4 times a given point.
_EXPECTED_POINTS_PER_READ = 1296
# As we disable commit log, we flush memory data to disk every so
# often to make sure they are persisted.
_FLUSH_MEMORY_EVERY_S = 15 * 60

_COMPONENTS_MAX_LEN = 64
_LAST_COMPONENT = "__END__"
_METADATA_CREATION_CQL_PATH_COMPONENTS = ", ".join(
    "component_%d text" % n for n in range(_COMPONENTS_MAX_LEN)
)
_METADATA_CREATION_CQL_METRICS = str(
    "CREATE TABLE IF NOT EXISTS \"%(keyspace)s\".metrics ("
    "  name text,"
    "  config map<text, text>,"
    "  " + _METADATA_CREATION_CQL_PATH_COMPONENTS + ","
    "  PRIMARY KEY (name)"
    ");"
)
_METADATA_CREATION_CQL_DIRECTORIES = str(
    "CREATE TABLE IF NOT EXISTS \"%(keyspace)s\".directories ("
    "  name text,"
    "  " + _METADATA_CREATION_CQL_PATH_COMPONENTS + ","
    "  PRIMARY KEY (name)"
    ");"
)
_METADATA_CREATION_CQL_PATH_INDEXES = [
    "CREATE CUSTOM INDEX IF NOT EXISTS ON \"%%(keyspace)s\".%(table)s (component_%(component)d)"
    "  USING 'org.apache.cassandra.index.sasi.SASIIndex'"
    "  WITH OPTIONS = {"
    "    'analyzer_class': 'org.apache.cassandra.index.sasi.analyzer.NonTokenizingAnalyzer',"
    "    'case_sensitive': 'true'"
    "  };" % {"table": t, "component": n}
    for t in 'metrics', 'directories'
    for n in range(_COMPONENTS_MAX_LEN)
]
_METADATA_CREATION_CQL = [
    _METADATA_CREATION_CQL_METRICS,
    _METADATA_CREATION_CQL_DIRECTORIES,
] + _METADATA_CREATION_CQL_PATH_INDEXES


_DATAPOINTS_CREATION_CQL_TEMPLATE = str(
    "CREATE TABLE IF NOT EXISTS %(table)s ("
    "  metric text,"              # Metric name.
    "  time_start_ms bigint,"     # Lower bound for this row.
    "  offset int,"               # time_start_ms + offset * precision = timestamp
    "  value double,"             # Value for the point.
    "  count int,"                # If value is sum, divide by count to get the avg.
    "  PRIMARY KEY ((metric, time_start_ms), offset)"
    ")"
    "  WITH CLUSTERING ORDER BY (offset DESC)"
    "  AND default_time_to_live = %(default_time_to_live)d"
    "  AND compaction = {"
    "    'class': 'DateTieredCompactionStrategy',"
    "    'base_time_seconds': '%(base_time_seconds)d',"
    "    'max_window_size_seconds': %(max_window_size_seconds)d,"
    "    'timestamp_resolution': 'MICROSECONDS'"
    "  };"
)


def _row_size_ms(stage):
    """Number of milliseconds to put in one Cassandra row.

    Args:
      stage: The stage the table stores.

    Returns:
      An integer, the duration in milliseconds.
    """
    return stage.precision_ms * _EXPECTED_POINTS_PER_READ


class _CappedConnection(c_asyncorereactor.AsyncoreConnection):
    """A connection with a cap on the number of in-flight requests per host."""

    # 300 is the minimum with protocol version 3, default is 65536
    max_in_flight = 300


class _LazyPreparedStatements(object):
    """On demand factory of prepared statements and tables.

    As per design (CASSANDRA_DESIGN.md) we have one table per retention stage.
    This creates tables and corresponding prepared statements once they are needed.
    """

    def __init__(self, session, keyspace):
        self._keyspace = keyspace
        self._session = session
        self.__stage_to_insert = {}
        self.__stage_to_select = {}

    def _create_datapoints_table(self, stage):
        # Time after which data expire.
        time_to_live = stage.duration + _OUT_OF_ORDER_S

        # Time it takes to receive a step
        arrival_time = stage.precision + _OUT_OF_ORDER_S

        # Estimate the age of the oldest data we still expect to read.
        fresh_time = stage.precision * _EXPECTED_POINTS_PER_READ

        # See http://www.datastax.com/dev/blog/datetieredcompactionstrategy
        #  - If too small: Reads need to touch many sstables
        #  - If too big: We pay compaction overhead for data that are never accessed anymore
        #    and get huge sstables
        # We set a minimum of arrival_time so that data are in order
        max_window_size_seconds = max(fresh_time, arrival_time + 1)

        statement_str = _DATAPOINTS_CREATION_CQL_TEMPLATE % {
            "table": self._get_table_name(stage),
            "default_time_to_live": time_to_live,
            # When we start compacting
            "base_time_seconds": arrival_time,
            "max_window_size_seconds": max_window_size_seconds,
            "memtable_flush_period_in_ms": _FLUSH_MEMORY_EVERY_S * 1000,
            "comment": "{\"created_by\": \"biggraphite\", \"schema_version\": 0}",
        }
        # The statement is idempotent
        self._session.execute(statement_str)

    def _get_table_name(self, stage):
        return "\"{}\".\"datapoints_{}p_{}s\"".format(self._keyspace, stage.points, stage.precision)

    def prepare_insert(self, stage, metric_name, time_start_ms, offset, value, count):
        statement = self.__stage_to_insert.get(stage)
        args = (metric_name, time_start_ms, offset, value, count)
        if statement:
            return statement, args

        self._create_datapoints_table(stage)
        statement_str = (
            "INSERT INTO %(table)s"
            " (metric, time_start_ms, offset, value, count)"
            " VALUES (?, ?, ?, ?, ?);"
        ) % {"table": self._get_table_name(stage)}
        statement = self._session.prepare(statement_str)
        statement.consistency_level = cassandra.ConsistencyLevel.ANY
        self.__stage_to_insert[stage] = statement
        return statement, args

    def prepare_select(self, stage, metric_name, row_start_ms, row_min_offset, row_max_offset):
        statement = self.__stage_to_select.get(stage)
        args = (metric_name, row_start_ms, row_min_offset, row_max_offset)
        if statement:
            return statement, args

        self._create_datapoints_table(stage)
        statement_str = (
            "SELECT time_start_ms, offset, value, count FROM %(table)s"
            " WHERE metric=? AND time_start_ms=?"
            " AND offset >= ? AND offset < ? "
            " ORDER BY offset;"
        ) % {"table": self._get_table_name(stage)}
        statement = self._session.prepare(statement_str)
        statement.consistency_level = cassandra.ConsistencyLevel.LOCAL_ONE
        self.__stage_to_select[stage] = statement
        return statement, args


class _CassandraAccessor(bg_accessor.Accessor):
    """Provides Read/Write accessors to Cassandra.

    Please refer to bg_accessor.Accessor.
    """

    # Current value is based on page settings, so that everything fits in a single Cassandra
    # reply with default settings.
    # TODO: Mesure actual number of metrics for existing queries and estimate a more
    # reasonable limit.
    MAX_METRIC_PER_GLOB = 5000

    _DEFAULT_CASSANDRA_PORT = 9042

    def __init__(self, keyspace='biggraphite', contact_points=[],
                 port=None, concurrency=4, default_timeout=None):
        """Record parameters needed to connect.

        Args:
          keyspace: Base names of Cassandra keyspaces dedicated to BigGraphite.
          contact_points: list of strings, the hostnames or IP to use to discover Cassandra.
          port: The port to connect to, as an int.
          concurrency: How many worker threads to use.
        """
        backend_name = "cassandra:" + keyspace
        super(_CassandraAccessor, self).__init__(backend_name)
        self.keyspace = keyspace
        self.keyspace_metadata = keyspace + "_metadata"
        self.contact_points = contact_points
        self.port = port or self._DEFAULT_CASSANDRA_PORT
        self.__concurrency = concurrency
        self.__downsampler = _downsampling.Downsampler()
        self.__cluster = None  # setup by connect()
        self.__lazy_statements = None  # setup by connect()
        self.__default_timeout = default_timeout
        self.__insert_metrics_statement = None  # setup by connect()
        self.__select_metric_statement = None  # setup by connect()
        self.__session = None  # setup by connect()

    def connect(self, skip_schema_upgrade=False):
        """See bg_accessor.Accessor."""
        super(_CassandraAccessor, self).connect(skip_schema_upgrade=skip_schema_upgrade)
        self.__cluster = c_cluster.Cluster(
            self.contact_points, self.port,
            executor_threads=self.__concurrency,
        )
        self.__cluster.connection_class = _CappedConnection  # Limits in flight requests
        self.__cluster.row_factory = c_query.tuple_factory  # Saves 2% CPU
        self.__session = self.__cluster.connect()
        if self.__default_timeout:
            self.__session.default_timeout = self.__default_timeout
        if not skip_schema_upgrade:
            self._upgrade_schema()

        self.__lazy_statements = _LazyPreparedStatements(self.__session, self.keyspace)

        # Metadata (metrics and directories)
        components_names = ", ".join("component_%d" % n for n in range(_COMPONENTS_MAX_LEN))
        components_marks = ", ".join("?" for n in range(_COMPONENTS_MAX_LEN))
        self.__insert_metrics_statement = self.__session.prepare(
            "INSERT INTO \"%s\".metrics (name, config, %s) VALUES (?, ?, %s);"
            % (self.keyspace_metadata, components_names, components_marks)
        )
        self.__insert_directories_statement = self.__session.prepare(
            "INSERT INTO \"%s\".directories (name, %s) VALUES (?, %s) IF NOT EXISTS;"
            % (self.keyspace_metadata, components_names, components_marks)
        )
        self.__select_metric_statement = self.__session.prepare(
            "SELECT config FROM \"%s\".metrics WHERE name = ?;" % self.keyspace_metadata
        )

        self.is_connected = True

    def create_metric(self, metric):
        """See bg_accessor.Accessor."""
        super(_CassandraAccessor, self).create_metric(metric)
        components = self._components_from_name(metric.name)
        queries = []

        # Check if parent dir exists. This is one round-trip but worthwile since otherwise
        # creating each parent directory requires a round-trip and the vast majority of
        # metrics have siblings.
        parent_dir = metric.name.rpartition(".")[0]
        if parent_dir and not self.glob_directory_names(parent_dir):
            # Create parent directories
            directory_path = []
            for component in components[:-2]:  # -1 for _LAST_COMPONENT, -1 for metric
                directory_path.append(component)
                directory_name = ".".join(directory_path)
                directory_components = directory_path + [_LAST_COMPONENT]
                directory_padding = [None] * (_COMPONENTS_MAX_LEN - len(directory_components))
                queries.append((
                    self.__insert_directories_statement,
                    [directory_name] + directory_components + directory_padding,
                ))

        # Finally, create the metric
        padding = [None] * (_COMPONENTS_MAX_LEN - len(components))
        metric_metadata_dict = metric.metadata.as_string_dict()
        queries.append((
            self.__insert_metrics_statement,
            [metric.name, metric_metadata_dict] + components + padding,
        ))

        # We have to run queries in sequence as:
        #  - we want them to have IF NOT EXISTS ease the hotspot on root directories
        #  - we do not want directories or metrics without parents (not handled by callee)
        #  - batch queries cannot contain IF NOT EXISTS and involve multiple primary keys
        # We can still end up with empty directories, which will need a reaper job to clean them.
        for statement, args in queries:
            self.__session.execute(statement, args)

    @staticmethod
    def _components_from_name(metric_name):
        res = metric_name.split(".")
        res.append(_LAST_COMPONENT)
        return res

    def drop_all_metrics(self):
        """See bg_accessor.Accessor."""
        super(_CassandraAccessor, self).drop_all_metrics()
        for keyspace in self.keyspace, self.keyspace_metadata:
            statement_str = "SELECT table_name FROM system_schema.tables WHERE keyspace_name = %s;"
            tables = [r[0] for r in self.__session.execute(statement_str, (keyspace, ))]
            for table in tables:
                self.__session.execute("TRUNCATE \"%s\".\"%s\";" % (keyspace, table))

    def fetch_points(self, metric, time_start, time_end, stage):
        """See bg_accessor.Accessor."""
        super(_CassandraAccessor, self).fetch_points(
            metric, time_start, time_end, stage)

        logging.debug(
            "fetch: [%s, start=%d, end=%d, stage=%s]",
            metric.name, time_start, time_end, stage)

        time_start_ms = int(time_start) * 1000
        time_end_ms = int(time_end) * 1000
        time_start_ms = max(time_end_ms - stage.duration_ms, time_start_ms)

        statements_and_args = self._fetch_points_make_selects(
            metric.name, time_start_ms, time_end_ms, stage)
        query_results = c_concurrent.execute_concurrent(
            self.__session,
            statements_and_args,
            concurrency=self.__concurrency,
            results_generator=True,
        )
        return bg_accessor.PointGrouper(
            metric, time_start_ms, time_end_ms, stage, query_results)

    def _fetch_points_make_selects(self, metric_name, time_start_ms,
                                   time_end_ms, stage):
        # We fetch with ms precision, even though we only store with second
        # precision.
        first_row = bg_accessor.round_down(time_start_ms, _row_size_ms(stage))
        last_row = bg_accessor.round_down(time_end_ms, _row_size_ms(stage))
        res = []
        # xrange(a,b) does not contain b, so we use last_row+1
        for row_start_ms in xrange(first_row, last_row + 1, _row_size_ms(stage)):
            row_min_offset_ms = -1  # Selects all FIXME
            row_max_offset_ms = stage.duration_ms  # Selects all
            if row_start_ms == first_row:
                row_min_offset_ms = time_start_ms - row_start_ms
            if row_start_ms == last_row:
                row_max_offset_ms = time_end_ms - row_start_ms
            row_min_offset = stage.step_ms(row_min_offset_ms)
            row_max_offset = stage.step_ms(row_max_offset_ms)

            select = self.__lazy_statements.prepare_select(
                stage=stage, metric_name=metric_name, row_start_ms=row_start_ms,
                row_min_offset=row_min_offset, row_max_offset=row_max_offset,
            )
            res.append(select)

        return res

    def get_metric(self, metric_name):
        """See bg_accessor.Accessor."""
        super(_CassandraAccessor, self).get_metric(metric_name)
        metric_name = bg_accessor.encode_metric_name(metric_name)
        result = list(self.__session.execute(
            self.__select_metric_statement, (metric_name, )))

        if not result:
            return None
        config = result[0][0]
        return bg_accessor.Metric(
            metric_name, bg_accessor.MetricMetadata.from_string_dict(config))

    def glob_directory_names(self, glob):
        """Return a sorted list of metric directories matching this glob."""
        super(_CassandraAccessor, self).glob_directory_names(glob)
        return self.__glob_names("directories", glob)

    def glob_metric_names(self, glob):
        """Return a sorted list of metric names matching this glob."""
        super(_CassandraAccessor, self).glob_metric_names(glob)
        return self.__glob_names("metrics", glob)

    def __glob_names(self, table, glob):
        components = self._components_from_name(glob)
        if len(components) > _COMPONENTS_MAX_LEN:
            msg = "Metric globs can have a maximum of %d dots" % _COMPONENTS_MAX_LEN - 2
            raise bg_accessor.InvalidGlobError(msg)

        where_parts = [
            "component_%d = %s" % (n, c_encoder.cql_quote(s))
            for n, s in enumerate(components)
            if s != "*"
        ]
        if len(where_parts) == len(components):
            # No wildcard, skip indexes
            where = "name = " + c_encoder.cql_quote(glob)
        else:
            where = " AND ".join(where_parts)
        query = (
            "SELECT name FROM \"%(keyspace)s\".\"%(table)s\""
            " WHERE %(where)s LIMIT %(limit)d ALLOW FILTERING;"
        ) % {
            "keyspace": self.keyspace_metadata, "table": table, "where": where,
            "limit": self.MAX_METRIC_PER_GLOB + 1,
        }
        try:
            metrics_names = [r[0] for r in self.__session.execute(query)]
        except Exception as e:
            raise RetryableCassandraError(e)
        if len(metrics_names) > self.MAX_METRIC_PER_GLOB:
            msg = "%s yields more than %d results" % (glob, self.MAX_METRIC_PER_GLOB)
            raise TooManyMetrics(msg)
        metrics_names.sort()
        return metrics_names

    def insert_points_async(self, metric, datapoints, on_done=None):
        """See bg_accessor.Accessor."""
        super(_CassandraAccessor, self).insert_points_async(
            metric, datapoints, on_done)

        logging.debug("insert: [%s, %s]", metric.name, datapoints)

        downsampled = self.__downsampler.feed(metric, datapoints)
        return self.insert_downsampled_points_async(metric, downsampled, on_done)

    def insert_downsampled_points_async(self, metric, downsampled, on_done=None):
        """See bg_accessor.Accessor."""
        if not downsampled and on_done:
            on_done(None)
            return

        count_down = None
        if on_done:
            count_down = _utils.CountDown(count=len(downsampled), on_zero=on_done)

        for timestamp, value, count, stage in downsampled:
            timestamp_ms = int(timestamp) * 1000
            time_offset_ms = timestamp_ms % _row_size_ms(stage)
            time_start_ms = timestamp_ms - time_offset_ms
            offset = stage.step_ms(time_offset_ms)

            statement, args = self.__lazy_statements.prepare_insert(
                stage=stage, metric_name=metric.name, time_start_ms=time_start_ms,
                offset=offset, value=value, count=count,
            )
            future = self.__session.execute_async(query=statement, parameters=args)
            if count_down:
                future.add_callbacks(
                    count_down.on_cassandra_result,
                    count_down.on_cassandra_failure,
                )

    def shutdown(self):
        """See bg_accessor.Accessor."""
        super(_CassandraAccessor, self).shutdown()
        if self.is_connected:
            try:
                self.__cluster.shutdown()
            except Exception as exc:
                raise CassandraError(exc)
            self.__cluster = None
            self.is_connected = False

    def _upgrade_schema(self):
        # Currently no change, so only upgrade operation is to setup
        try:
            self.__session.execute("SELECT name FROM directories LIMIT 1;")
            return  # Already up to date.
        except Exception:
            pass
        for cql in _METADATA_CREATION_CQL:
            self.__session.execute(cql % {"keyspace": self.keyspace_metadata})


def build(*args, **kwargs):
    """Return a bg_accessor.Accessor using Casssandra.

    Args:
      keyspace: Base name of Cassandra keyspaces dedicated to BigGraphite.
      contact_points: list of strings, the hostnames or IP to use to discover Cassandra.
      port: The port to connect to, as an int.
      concurrency: How many worker threads to use.
    """
    return _CassandraAccessor(*args, **kwargs)
