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
from __future__ import print_function

import json

import cassandra
from cassandra import cluster as c_cluster
from cassandra import concurrent as c_concurrent
from cassandra import encoder as c_encoder
import statistics


class Error(Exception):
    """Base class for all exceptions from this module."""


class CassandraError(Error):
    """Fatal errors accessing Cassandra."""


class NotConnectedError(CassandraError):
    """Fatal errors accessing Cassandra because the accessor is not connected."""


class RetryableCassandraError(Error):
    """Errors accessing Cassandra that could succeed if retried."""


class InvalidArgumentError(Error):
    """Callee did not follow requirements on the arguments."""


class InvalidGlobError(InvalidArgumentError):
    """The provided glob is invalid."""


class TooManyMetrics(InvalidArgumentError):
    """A name glob yielded more than Accessor.MAX_METRIC_PER_GLOB metrics."""


_COMPONENTS_MAX_LEN = 64
_LAST_COMPONENT = "__END__"
# THE "METRICS" TABLE
# To resolve globs, we store metrics in a table that among other
# things contain 64 columns, one for each path component.
# The metric "a.b.c" is therefore indexed as:
#   name="a.b.c", config=...,
#   component_0="a", component_1="b", component_2="c", component_3="__END__",
#   component_4=NULL, component_5=NULL, ..., component_63=NULL
# A SASI Index is declared on each of the component columns. We use SASI
# because it knows to process multi-column queries without filtering.
#
# The way it works is by indexing tokens in a B+Tree for each column of
# each sstable.
# After finding the right places in the B+Trees, SASI picks the column with
# the least results, and merge the result together. It uses multiple lookups
# if one column is 100 times bigger than the other, otherwise it uses a merge join.
#
# SASI support for LIKE queries is limited to finding substrings.
# So when resolving a query like a.x*z.c we query for a.*.c and then do
# client-side filtering.
#
# For more details on SASI: https://github.com/apache/cassandra/blob/trunk/doc/SASI.md
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
# THE "DIRECTORIES" TABLE
# Similar to the metrics table, we create for parents of all metrics an entry in a
# directory table. It is used to implement the 'metric' API in graphite.
#
# Directories are implicitely created before metrics by the Accessor.create_metric()
# function. Because of this, it is possible to end up with an empty directory (if the
# program crashes). A cleaner job will be needed if they accumulate for too long.
#
# WHY TWO DIFFERENT TABLES?
# Pros of 2 tables:
#  - We expect them to end up with increasingly different metadata (quotas on directories and
#    whisper-header-like on metrics) and the semantic of a merged entry will become complex
#  - We suspect that implementing quota or cleaning of empty directories will be easier if
#    we can iterate on directories without filtering all metrics
# Pros of 1 table:
#  - We get batch transactions to update metadata at the same time as parent entries
#  - We could not need an additional parameter for functions that use fields presents in
#    both metrics and directories (but I expect they will diverge later to make the return
#    type not depend on the argument)
#  - We could still build a materialized view of directories only to get performance
#    benefits above
# None of the points above is a strong decision factor, but merging things later is generally
# easier than untangling them so we keept them separate for now.
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
# THE "RAW" TABLE
# To allow for efficient compaction, we use the DateTieredCompactionStrategy, "DTCS".
# Its usage in the context of timeserie databases is described at:
# http://www.datastax.com/dev/blog/datetieredcompactionstrategy
#
# Another trick we use is grouping related timestamps (_ROW_SIZE_MS)
# in the same row described by time_start_ms and using time_offset_ms to describe
# a delta from it. This saves space in two ways:
#  - No need to repeat metric names on each row.
#  - The relative time offset is 4 bytes only when a timestamp would be 8.
_ROW_SIZE_MS = 3600 * 1000
_SETUP_CQL_DATAPOINTS = str(
    "CREATE TABLE IF NOT EXISTS datapoints ("
    "  metric text,"
    "  time_start_ms bigint,"
    "  time_offset_ms int,"
    "  value double,"
    # TODO: Add aggregation_step
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


class MetricMetadata(object):
    """Represents all information about a metric."""

    __slots__ = (
        "carbon_aggregation", "carbon_highest_precision",
        "carbon_retentions", "carbon_xfilesfactor", "name",
    )

    _DEFAULT_AGGREGATION = "average"
    _DEFAULT_RETENTIONS = [[1, 24*3600]]  # One second for one day
    _DEFAULT_XFILESFACTOR = 0.5

    def __init__(self, name,
                 carbon_aggregation=None, carbon_retentions=None, carbon_xfilesfactor=None):
        """Record its arguments."""
        self.name = name
        self.carbon_aggregation = carbon_aggregation or self._DEFAULT_AGGREGATION
        self.carbon_retentions = carbon_retentions or self._DEFAULT_RETENTIONS
        self.carbon_xfilesfactor = carbon_xfilesfactor
        if self.carbon_xfilesfactor is None:
            self.carbon_xfilesfactor = self._DEFAULT_XFILESFACTOR
        self.carbon_highest_precision = self.carbon_retentions[0][0]

    def as_string_dict(self):
        """Turn an instance into a dict of string to string."""
        return {
            "carbon_aggregation": self.carbon_aggregation,
            "carbon_retentions": json.dumps(self.carbon_retentions),
            "carbon_xfilesfactor": "%f" % self.carbon_xfilesfactor,
        }

    @classmethod
    def from_string_dict(cls, d):
        """Turn a dict of string to string into a MetricMetadata."""
        return cls(
            name=d["name"],
            carbon_aggregation=d.get("carbon_aggregation"),
            carbon_retentions=json.loads(d.get("carbon_retentions")),
            carbon_xfilesfactor=float(d.get("carbon_xfilesfactor")),
        )

    def carbon_aggregate_points(self, time_span, points):
        """"An aggregator function suitable for Accessor.fetch().

        Args:
          time_span: the duration for which we are aggregating, in seconds.
            For example, if points are meant to represent an hour, the value is 3600.
          points: values to aggregate as float from most recent to oldest

        Returns:
          A float, or None to reject the points.
        """
        # TODO: Handle precomputation of aggregates.
        assert time_span
        coverage = len(points) * self.carbon_highest_precision / float(time_span)
        if not points or coverage < self.carbon_xfilesfactor:
            return None
        if self.carbon_aggregation == "average":
            return float(sum(points)) / len(points)
        if self.carbon_aggregation == "last":
            # Points are stored in descending order, the "last" is actually the first
            return points[0]
        if self.carbon_aggregation == "min":
            return min(points)
        if self.carbon_aggregation == "max":
            return max(points)
        if self.carbon_aggregation == "sum":
            return sum(points)
        raise InvalidArgumentError("Unknown aggregation method: %s" % self.carbon_aggregation)


class Accessor(object):
    """Provides Read/Write accessors to BigGraphite.

    It is safe to fork() or start new process until connect() has been called.
    It is not safe to share a given accessor across threads.
    Calling a method that requires a connection without being connected() will result
    in NotConnectedError being raised.
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

    def __enter__(self):
        """Call connect()."""
        self.connect()
        return self

    def __exit__(self, _type, _value, _traceback):
        """Call shutdown()."""
        self.shutdown()
        return False

    def _upgrade_schema(self):
        # Currently no change, so only upgrade operation is to setup
        try:
            self.__session.execute("SELECT name FROM directories LIMIT 1;")
            return  # Already up to date.
        except Exception:
            pass
        for cql in _SETUP_CQL:
            self.__session.execute(cql)

    def connect(self, skip_schema_upgrade=False):
        """Establish a connection to Cassandra.

        This must be called AFTER creating subprocess with the multiprocessing module.
        """
        if self.is_connected:
            return
        try:
            self._connect(skip_schema_upgrade=skip_schema_upgrade)
        except Exception as exc:
            try:
                self.shutdown()
            except Error:
                self.__cluster = None
            raise CassandraError(exc)

    def shutdown(self):
        """Close the connection to Cassandra."""
        if self.is_connected:
            try:
                self.__cluster.shutdown()
            except Exception as exc:
                raise CassandraError(exc)
            self.__cluster = None

    @staticmethod
    def _components_from_name(metric_name):
        res = metric_name.split(".")
        res.append(_LAST_COMPONENT)
        return res

    def _connect(self, skip_schema_upgrade=False):
        executor_threads = self._round_up(
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
            "SELECT name, config FROM metrics WHERE name = ?;"
        )

    def _make_insert_points(self, metric_name, timestamp, value):
        # We store only one point per second, though the on-disk structure allow for more.
        timestamp_ms = int(timestamp) * 1000
        time_offset_ms = timestamp_ms % _ROW_SIZE_MS
        time_start_ms = timestamp_ms - time_offset_ms
        return (self.__insert_statement, (metric_name, time_start_ms, time_offset_ms, value))

    def insert_points(self, metric_name, timestamps_and_values):
        """Insert points for a given metric.

        Args:
          metric_name: A graphite-like metric name (like "my.own.metric")
          timestamps_and_values: An iterable of (timestamp in seconds, values as double)
        """
        self._check_connected()
        statements_and_args = [
            self._make_insert_points(metric_name, t, v)
            for t, v in timestamps_and_values
        ]
        c_concurrent.execute_concurrent(
            self.__session, statements_and_args, concurrency=self.__concurrency,
        )

    def drop_all_metrics(self):
        """Delete all metrics from the database."""
        self._check_connected()
        self.__session.execute("TRUNCATE datapoints;")
        self.__session.execute("TRUNCATE directories;")
        self.__session.execute("TRUNCATE metrics;")

    @staticmethod
    def _round_down(rounded, devider):
        return int(rounded) // devider * devider

    @staticmethod
    def _round_up(rounded, devider):
        return int(rounded + devider - 1) // devider * devider

    def _make_all_points_selects(self, metric_name, time_start_ms, time_end_ms):
        # We fetch with ms precision, even though we only store with second precision.
        first_row = self._round_down(time_start_ms, _ROW_SIZE_MS)
        last_row = self._round_down(time_end_ms, _ROW_SIZE_MS)
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

    def create_metric(self, metric_metadata):
        """Create a metric definition from a MetricMetadata.

        Parent directory are implicitly created.
        As this requires O(10) sequential inserts, it is worthwile to first check
        if the metric exists and not recreate it if it does.

        Args:
          metric_metadata: The metric definition.
        """
        self._check_connected()
        metric_name = metric_metadata.name
        components = self._components_from_name(metric_name)
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
        queries.append((
            self.__insert_metrics_statement,
            [metric_name, metric_metadata.as_string_dict()] + components + padding,
        ))

        # We have to run queries in sequence as:
        #  - we want them to have IF NOT EXISTS ease the hotspot on root directories
        #  - we do not want directories or metrics without parents (not handled by callee)
        #  - batch queries cannot contain IF NOT EXISTS and involve multiple primary keys
        # We can still end up with empty directories, which will need a reaper job to clean them.
        for statement, args in queries:
            self.__session.execute(statement, args)

    @staticmethod
    def median_aggregator(time_span, points):
        """The default aggregator for fetch(), returns the median of points."""
        if points:
            return statistics.median(points)
        else:
            return None

    def get_metric(self, metric_name):
        """Return a MetricMetadata for this metric_name, None if no such metric."""
        self._check_connected()
        result = list(self.__session.execute(self.__select_metric_statement, (metric_name, )))
        if not result:
            return None
        name = result[0][0]
        config = result[0][1]
        config["name"] = name
        return MetricMetadata.from_string_dict(config)

    # TODO: handle ranges and the like
    # http://graphite.readthedocs.io/en/latest/render_api.html#paths-and-wildcards
    # TODO: Handled subdirectories for the graphite-web API
    def glob_metric_names(self, glob):
        """Return a sorted list of metric names matching this glob."""
        self._check_connected()
        return self.__glob_names("metrics", glob)

    def glob_directory_names(self, glob):
        """Return a sorted list of metric directories matching this glob."""
        self._check_connected()
        return self.__glob_names("directories", glob)

    def __glob_names(self, table, glob):
        components = self._components_from_name(glob)
        if len(components) > _COMPONENTS_MAX_LEN:
            msg = "Metric globs can have a maximum of %d dots" % _COMPONENTS_MAX_LEN - 2
            raise InvalidGlobError(msg)

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

    # TODO: Remove aggregator_func and perform aggregation server-side.
    def fetch_points(self, metric_name, time_start, time_end, step,
                     aggregator_func=None, _fake_query_results=None):
        """Fetch points from time_start included to time_end excluded with a duration of step.

        connect() must have previously been called.

        Args:
          time_start: timestamp in second from the Epoch as an int, inclusive,
            must be a multiple of step
          time_end: timestamp in second from the Epoch as an int, exclusive,
            must be a multiple of step
          step: time delta in seconds as an int, must be > 0
          aggregator_func: (time_span, values...)->value called to aggregate
            values regarding a single step. defaults to self.median_aggregator().
            Args:
              time_span: the duration for which we are aggregating in seconds
              values: a list of float
            Returns:
              either a float or None to reject the step
          _fake_query_results: Only meant for test_utils.FakeAccessor, considered to be
            an implementation detail and may go away at any time.

        Returns:
          a list of (timestamp, value) where timestamp indicates the value is about the
          range from timestamp included to timestamp+step excluded

        Raises:
          InvalidArgumentError: if time_start, time_end or step are not as per above
        """
        self._fetch_points_validate_args(metric_name, time_start, time_end, step, aggregator_func)
        aggregator_func = aggregator_func or self.median_aggregator
        step_ms = int(step) * 1000
        time_start_ms = int(time_start) * 1000
        time_end_ms = int(time_end) * 1000
        time_start_ms = max(time_end_ms - self._MAX_QUERY_RANGE_MS, time_start_ms)
        statements_and_args = self._make_all_points_selects(metric_name, time_start_ms, time_end_ms)
        if _fake_query_results:
            query_results = _fake_query_results
        else:
            self._check_connected()
            query_results = c_concurrent.execute_concurrent(
                self.__session, statements_and_args, concurrency=self.__concurrency,
                results_generator=True,
            )

        first_exc = None
        current_points = []
        current_timestamp_ms = None
        res = []

        def add_current_points_to_res():
            aggregate = aggregator_func(step, current_points)
            if aggregate is not None:
                res.append((current_timestamp_ms / 1000.0, aggregate))

        for successfull, rows_or_exception in query_results:
            if first_exc:
                continue  # A query failed, we still consume the results
            if not successfull:
                first_exc = rows_or_exception
            for row in rows_or_exception:
                timestamp_ms = row[0] + row[1]
                assert timestamp_ms >= time_start_ms
                assert timestamp_ms < time_end_ms
                timestamp_ms = self._round_down(timestamp_ms, step_ms)

                if current_timestamp_ms != timestamp_ms:
                    if current_timestamp_ms is not None:
                        # This is the first point we encounter, do not emit it on its own,
                        # rather wait until we have found points fitting in the next period.
                        add_current_points_to_res()
                    current_timestamp_ms = timestamp_ms
                    del current_points[:]

                current_points.append(row[2])

        if first_exc:
            raise RetryableCassandraError(first_exc)

        if current_timestamp_ms is not None:
            # We haven't encountered any point.
            add_current_points_to_res()
        return res

    @property
    def is_connected(self):
        """Return true if connect() has been called since last shutdown()."""
        return bool(self.__cluster)

    def _check_connected(self):
        if not self.is_connected:
            raise NotConnectedError("Accessor's connect() wasn't called")

    @staticmethod
    def _fetch_points_validate_args(metric_name, time_start, time_end, step, aggregator_func):
        """Check arguments as per fetch() spec, raises InvalidArgumentError if needed."""
        if step < 1:
            raise InvalidArgumentError("step (%d) is not positive" % step)
        if time_start % step or time_start < 0:
            raise InvalidArgumentError("time_start (%d) is not a multiple of step (%d)"
                                       % (time_start, step))
        if time_end % step or time_end < 0:
            raise InvalidArgumentError("time_end (%d) is not a multiple of step (%d)"
                                       % (time_end, step))
