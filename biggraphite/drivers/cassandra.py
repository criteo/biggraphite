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

import threading

import cassandra
from cassandra import cluster as c_cluster
from cassandra import concurrent as c_concurrent
from cassandra import encoder as c_encoder

from biggraphite import accessor as bg_accessor


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


_COMPONENTS_MAX_LEN = 64
_LAST_COMPONENT = "__END__"
_SETUP_CQL_PATH_COMPONENTS = ", ".join(
    "component_%d text" % n for n in range(_COMPONENTS_MAX_LEN)
)
_SETUP_CQL_METRICS = str(
    "CREATE TABLE IF NOT EXISTS metrics ("
    "  name text,"
    "  config map<text, text>,"
    "  " + _SETUP_CQL_PATH_COMPONENTS + ","
    "  PRIMARY KEY (name)"
    ");"
)
_SETUP_CQL_DIRECTORIES = str(
    "CREATE TABLE IF NOT EXISTS directories ("
    "  name text,"
    "  " + _SETUP_CQL_PATH_COMPONENTS + ","
    "  PRIMARY KEY (name)"
    ");"
)
_SETUP_CQL_PATH_INDEXES = [
    "CREATE CUSTOM INDEX IF NOT EXISTS ON %s (component_%d)"
    "  USING 'org.apache.cassandra.index.sasi.SASIIndex'"
    "  WITH OPTIONS = {"
    "    'analyzer_class': 'org.apache.cassandra.index.sasi.analyzer.NonTokenizingAnalyzer',"
    "    'case_sensitive': 'true'"
    "  };" % (t, n)
    for t in 'metrics', 'directories'
    for n in range(_COMPONENTS_MAX_LEN)
]
_ROW_SIZE_MS = 3600 * 1000
_SETUP_CQL_DATAPOINTS = str(
    "CREATE TABLE IF NOT EXISTS datapoints ("
    "  metric text,"
    "  time_start_ms bigint,"
    "  time_offset_ms int,"
    "  value double,"
    "  PRIMARY KEY ((metric, time_start_ms), time_offset_ms)"
    ")"
    "  WITH CLUSTERING ORDER BY (time_offset_ms DESC)"
    "  AND compaction={"
    "    'timestamp_resolution': 'MICROSECONDS',"
    "    'max_sstable_age_days': '1',"
    "    'base_time_seconds': '900',"
    "    'class': 'DateTieredCompactionStrategy'"
    "  };"
)
_SETUP_CQL = [
    _SETUP_CQL_DATAPOINTS,
    _SETUP_CQL_METRICS,
    _SETUP_CQL_DIRECTORIES,
] + _SETUP_CQL_PATH_INDEXES


class _CountDown(object):
    """Decrements a count, calls a callback when it reaches 0."""

    __slots__ = ("_canceled", "_count", "_lock", "_on_zero", )

    def __init__(self, count, on_zero):
        """Record parameters."""
        self._canceled = False
        self._count = count
        self._lock = threading.Lock()
        self._on_zero = on_zero

    def cancel(self, reason):
        """Call the callback now with reason as argument."""
        with self._lock:
            self._canceled = True
            self._on_done(reason)

    def decrement(self):
        """Call the callback if count reached zero, with None as argument."""
        with self._lock:
            self._count -= 1
            if not self._count and not self._canceled:
                self._on_zero(None)

    def on_cassandra_result(self, unused_result):
        """Call decrement(), suitable for execute_async."""
        self.decrement()

    def on_cassandra_failure(self, exc):
        """Call cancel(), suitable for execute_async."""
        self.cancel(Error(exc))


class _CassandraAccessor(bg_accessor.Accessor):
    """Provides Read/Write accessors to Cassandra.

    Please refer to bg_accessor.Accessor.
    """

    _MAX_QUERY_RANGE_MS = 365 * 24 * _ROW_SIZE_MS

    # Current value is based on page settings, so that everything fits in a single Cassandra
    # reply with default settings.
    # TODO: Mesure actual number of metrics for existing queries and estimate a more
    # reasonable limit.
    MAX_METRIC_PER_GLOB = 5000
    # This is taken from the default Cassandra binding that runs 0 concurrent inserts
    # with 2 threads.
    _REQUESTS_PER_THREAD = 50.0

    _DEFAULT_CASSANDRA_PORT = 9042

    def __init__(self, keyspace, contact_points, port=None, concurrency=4):
        """Record parameters needed to connect.

        Args:
          keyspace: Name of Cassandra keyspace dedicated to BigGraphite.
          contact_points: list of strings, the hostnames or IP to use to discover Cassandra.
          port: The port to connect to, as an int.
          concurrency: How many worker threads to use.
        """
        backend_name = "cassandra:" + keyspace
        super(_CassandraAccessor, self).__init__(backend_name)
        self.keyspace = keyspace
        self.contact_points = contact_points
        self.port = port or self._DEFAULT_CASSANDRA_PORT
        self.__concurrency = concurrency
        self.__cluster = None  # setup by connect()
        self.__insert_metrics_statement = None  # setup by connect()
        self.__insert_statement = None  # setup by connect()
        self.__select_metric_statement = None  # setup by connect()
        self.__select_statement = None  # setup by connect()
        self.__session = None  # setup by connect()

    def connect(self, skip_schema_upgrade=False):
        """See bg_accessor.Accessor."""
        super(_CassandraAccessor, self).connect(skip_schema_upgrade=skip_schema_upgrade)
        executor_threads = bg_accessor.round_up(
            self.__concurrency, self._REQUESTS_PER_THREAD) / self._REQUESTS_PER_THREAD
        self.__cluster = c_cluster.Cluster(
            self.contact_points, self.port, executor_threads=executor_threads,
        )
        self.__session = self.__cluster.connect()
        self.__session.set_keyspace(self.keyspace)
        if not skip_schema_upgrade:
            self._upgrade_schema()
        self.__insert_statement = self.__session.prepare(
            "INSERT INTO datapoints (metric, time_start_ms, time_offset_ms, value)"
            " VALUES (?, ?, ?, ?);")
        self.__insert_statement.consistency_level = cassandra.ConsistencyLevel.ONE
        self.__select_statement = self.__session.prepare(
            "SELECT time_start_ms, time_offset_ms, value FROM datapoints"
            " WHERE metric=? AND time_start_ms=? "
            " AND time_offset_ms >= ? AND time_offset_ms < ? "
            " ORDER BY time_offset_ms;")
        components_names = ", ".join("component_%d" % n for n in range(_COMPONENTS_MAX_LEN))
        components_marks = ", ".join("?" for n in range(_COMPONENTS_MAX_LEN))
        self.__insert_metrics_statement = self.__session.prepare(
            "INSERT INTO metrics (name, config, %s) VALUES (?, ?, %s);"
            % (components_names, components_marks)
        )
        self.__insert_directories_statement = self.__session.prepare(
            "INSERT INTO directories (name, %s) VALUES (?, %s) IF NOT EXISTS;"
            % (components_names, components_marks)
        )
        self.__select_metric_statement = self.__session.prepare(
            "SELECT config FROM metrics WHERE name = ?;"
        )
        self.is_connected = True

    def create_metric(self, metric):
        """See bg_accessor.Accessor."""
        super(_CassandraAccessor, self).create_metric(metric)
        components = self._components_from_name(metric.name)
        queries = []

        directory_path = []
        # Create parent directories
        for component in components[:-2]:  # -1 for _LAST_COMPONENT, -1 for metric
            directory_path.append(component)
            directory_name = ".".join(directory_path)
            directory_components = directory_path + [_LAST_COMPONENT]
            directory_padding = [None] * (_COMPONENTS_MAX_LEN - len(directory_components))
            queries.append((
                self.__insert_directories_statement,
                [directory_name] + directory_components + directory_padding,
            ))
        padding = [None] * (_COMPONENTS_MAX_LEN - len(components))
        # Finally, create the metric
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
        self.__session.execute("TRUNCATE datapoints;")
        self.__session.execute("TRUNCATE directories;")
        self.__session.execute("TRUNCATE metrics;")

    # TODO: Perform aggregation server-side.
    def fetch_points(self, metric, time_start, time_end, step):
        """See bg_accessor.Accessor."""
        super(_CassandraAccessor, self).fetch_points(metric, time_start, time_end, step)
        step_ms = int(step) * 1000
        time_start_ms = int(time_start) * 1000
        time_end_ms = int(time_end) * 1000
        time_start_ms = max(time_end_ms - self._MAX_QUERY_RANGE_MS, time_start_ms)

        statements_and_args = self._fetch_points_make_selects(
            metric.name, time_start_ms, time_end_ms)

        query_results = c_concurrent.execute_concurrent(
            self.__session,
            statements_and_args,
            concurrency=self.__concurrency,
            results_generator=True,
        )
        return bg_accessor.PointGrouper(metric, time_start_ms, time_end_ms, step_ms, query_results)

    def _fetch_points_make_selects(self, metric_name, time_start_ms, time_end_ms):
        # We fetch with ms precision, even though we only store with second precision.
        first_row = bg_accessor.round_down(time_start_ms, _ROW_SIZE_MS)
        last_row = bg_accessor.round_down(time_end_ms, _ROW_SIZE_MS)
        res = []
        # xrange(a,b) does not contain b, so we use last_row+1
        for row_start_ms in xrange(first_row, last_row + 1, _ROW_SIZE_MS):
            row_min_offset = -1  # Selects all
            row_max_offset = _ROW_SIZE_MS + 1   # Selects all
            if row_start_ms == first_row:
                row_min_offset = time_start_ms - row_start_ms
            if row_start_ms == last_row:
                row_max_offset = time_end_ms - row_start_ms
            statement = (self.__select_statement,
                         (metric_name, row_start_ms, row_min_offset, row_max_offset))
            res.append(statement)
        return res

    def get_metric(self, metric_name):
        """See bg_accessor.Accessor."""
        super(_CassandraAccessor, self).get_metric(metric_name)
        metric_name = bg_accessor.encode_metric_name(metric_name)
        result = list(self.__session.execute(self.__select_metric_statement, (metric_name, )))
        if not result:
            return None
        config = result[0][0]
        return bg_accessor.Metric(metric_name, bg_accessor.MetricMetadata.from_string_dict(config))

    def glob_directory_names(self, glob):
        """Return a sorted list of metric directories matching this glob."""
        super(_CassandraAccessor, self).glob_directory_names(glob)
        return self.__glob_names("directories", glob)

    # TODO: handle ranges and the like
    # http://graphite.readthedocs.io/en/latest/render_api.html#paths-and-wildcards
    # TODO: Handled subdirectories for the graphite-web API
    def glob_metric_names(self, glob):
        """Return a sorted list of metric names matching this glob."""
        super(_CassandraAccessor, self).glob_metric_names(glob)
        return self.__glob_names("metrics", glob)

    def __glob_names(self, table, glob):
        components = self._components_from_name(glob)
        if len(components) > _COMPONENTS_MAX_LEN:
            msg = "Metric globs can have a maximum of %d dots" % _COMPONENTS_MAX_LEN - 2
            raise bg_accessor.InvalidGlobError(msg)

        where = [
            "component_%d = %s" % (n, c_encoder.cql_quote(s))
            for n, s in enumerate(components)
            if s != "*"
        ]
        query = " ".join([
            "SELECT name FROM %s WHERE" % table,
            " AND ".join(where),
            "LIMIT %d ALLOW FILTERING;" % (self.MAX_METRIC_PER_GLOB + 1),
        ])
        try:
            metrics_names = [r.name for r in self.__session.execute(query)]
        except Exception as e:
            raise RetryableCassandraError(e)
        if len(metrics_names) > self.MAX_METRIC_PER_GLOB:
            msg = "%s yields more than %d results" % (glob, self.MAX_METRIC_PER_GLOB)
            raise TooManyMetrics(msg)
        metrics_names.sort()
        return metrics_names

    def insert_points_async(self, metric, timestamps_and_values, on_done=None):
        """See bg_accessor.Accessor."""
        super(_CassandraAccessor, self).insert_points_async(metric, timestamps_and_values, on_done)
        count_down = None
        if on_done:
            count_down = _CountDown(count=len(timestamps_and_values), on_zero=on_done)

        for timestamp, value in timestamps_and_values:
            timestamp_ms = int(timestamp) * 1000
            time_offset_ms = timestamp_ms % _ROW_SIZE_MS
            time_start_ms = timestamp_ms - time_offset_ms
            args = (metric.name, time_start_ms, time_offset_ms, value, )

            future = self.__session.execute_async(self.__insert_statement, args)
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
        for cql in _SETUP_CQL:
            self.__session.execute(cql)


def connect(*args, **kwargs):
    """Return a bg_accessor.Accessor connected to Casssandra.

    Args:
      keyspace: Name of Cassandra keyspace dedicated to BigGraphite.
      contact_points: list of strings, the hostnames or IP to use to discover Cassandra.
      port: The port to connect to, as an int.
      concurrency: How many worker threads to use.
    """
    return _CassandraAccessor(*args, **kwargs)
