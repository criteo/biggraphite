#!/usr/bin/env python
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


class RetryableCassandraError(Error):
    """Errors accessing Cassandra that could succeed if retried."""


class InvalidArgumentError(Error):
    """Callee did not follow requirements on the arguments."""


class InvalidGlobError(InvalidArgumentError):
    """The provided glob is invalid."""


class TooManyMetrics(InvalidArgumentError):
    """A name glob yielded more than Accessor.MAX_METRIC_PER_GLOB metrics."""


_LAST_COMPONENT = '__END__'

_COMPONENTS_MAX_LEN = 64

_SETUP_CQL_METRICS_COMPONENTS = "\n".join(
    "  component_%d text," % n for n in range(_COMPONENTS_MAX_LEN)
)

_SETUP_CQL_METRICS_INDEXES = [
    "CREATE CUSTOM INDEX IF NOT EXISTS ON metrics (component_%d)"
    "  USING 'org.apache.cassandra.index.sasi.SASIIndex'"
    "  WITH OPTIONS = {"
    "    'analyzer_class': 'org.apache.cassandra.index.sasi.analyzer.NonTokenizingAnalyzer',"
    "    'case_sensitive': 'true'"
    "  };" % n for n in range(_COMPONENTS_MAX_LEN)
]

_SETUP_CQL = [
    "CREATE TABLE IF NOT EXISTS raw ("
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
    "  };",
    "CREATE TABLE IF NOT EXISTS metrics ("
    "  name text,"
    "  config map<text, text>,"
    "" + _SETUP_CQL_METRICS_COMPONENTS + ""
    "  PRIMARY KEY (name)"
    ");",
] + _SETUP_CQL_METRICS_INDEXES


class MetricMetadata(object):
    """Represents all information about a metric."""

    _DEFAULT_AGGREGATION = "average"
    _DEFAULT_RETENTIONS = [[1, 24*3600]]  # One second for one day

    def __init__(self, name,
                 carbon_aggregation=None, carbon_retentions=None, carbon_xfilesfactor=None):
        """Record its arguments."""
        self.name = name
        self.carbon_aggregation = carbon_aggregation or self._DEFAULT_AGGREGATION
        self.carbon_retentions = carbon_retentions or self._DEFAULT_RETENTIONS
        self.carbon_xfilesfactor = carbon_xfilesfactor
        if self.carbon_xfilesfactor is None:
            self.carbon_xfilesfactor = 0.5

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

    def carbon_aggregate_points(self, coverage, points):
        """"An aggregator function suitable for Accessor.fetch().

        Args:
          coverage: the proportion of time for which we have values, as a float
          points: values to aggregate as float

        Returns:
          A float, or None to reject the points.01
        """
        if not points:
            return points
        if coverage < self.carbon_xfilesfactor:
            return None
        if self.carbon_aggregation == "average":
            return sum(points) / len(points)
        if self.carbon_aggregation == "last":
            # Points are stored in descending order, the "last" is actually the first
            return points[0]
        if self.carbon_aggregation == "min":
            return min(points)
        if self.carbon_aggregation == "max":
            return max(points)
        if self.carbon_aggregation == "sum":
            return sum(points)


class Accessor(object):
    """Provides Read/Write accessors to BigGraphite.

    It is safe to fork() or start new process until connect() has been called.
    It is not safe to share a given accessor across threads.
    """

    _ROW_SIZE_MS = 3600 * 1000
    _MAX_QUERY_RANGE_MS = 365 * 24 * _ROW_SIZE_MS

    MAX_METRIC_PER_GLOB = 1000

    def __init__(self, keyspace, contact_points, port=9042, executor_threads=4):
        """Record parameters needed to connect.

        Args:
          keyspace: Name of Cassandra keyspace dedicated to BigGraphite.
          contact_points: list of strings, the hostnames or IP to use to discover Cassandra.
          port: The port to connect to, as an int.
          executor_threads: How many worker threads to use.
        """
        self.keyspace = keyspace
        self.contact_points = contact_points
        self.port = port
        self.__executor_threads = executor_threads
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
        for cql in _SETUP_CQL:
            self.__session.execute(cql)

    def connect(self, skip_schema_upgrade=False):
        """Establish a connection to Cassandra.

        This must be called AFTER creating subprocess with the multiprocessing module.
        """
        if self.__cluster:
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
        if self.__cluster:
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
        self.__cluster = c_cluster.Cluster(
            self.contact_points, self.port, executor_threads=self.__executor_threads)
        self.__session = self.__cluster.connect()
        self.__session.set_keyspace(self.keyspace)
        if not skip_schema_upgrade:
            self._upgrade_schema()
        self.__insert_statement = self.__session.prepare(
            "INSERT INTO raw (metric, time_start_ms, time_offset_ms, value)"
            " VALUES (?, ?, ?, ?);")
        self.__insert_statement.consistency_level = cassandra.ConsistencyLevel.ONE
        self.__select_statement = self.__session.prepare(
            "SELECT time_start_ms, time_offset_ms, value FROM raw"
            " WHERE metric=? AND time_start_ms=? "
            " AND time_offset_ms >= ? AND time_offset_ms < ? "
            " ORDER BY time_offset_ms;")
        components_names = ", ".join("component_%d" % n for n in range(_COMPONENTS_MAX_LEN))
        components_marks = ", ".join("?" for n in range(_COMPONENTS_MAX_LEN))
        self.__insert_metrics_statement = self.__session.prepare(
            "INSERT INTO metrics (name, config, %s) VALUES (?, ?, %s);"
            % (components_names, components_marks)
        )
        self.__select_metric_statement = self.__session.prepare(
            "SELECT name, config FROM metrics WHERE name = ?;"
        )

    def _make_insert_points(self, metric_name, timestamp, value):
        # We store only one point per second, though the on-disk structure allow for more.
        timestamp_ms = int(timestamp) * 1000
        time_offset_ms = timestamp_ms % self._ROW_SIZE_MS
        time_start_ms = timestamp_ms - time_offset_ms
        return (self.__insert_statement, (metric_name, time_start_ms, time_offset_ms, value))

    def insert_points(self, metric_name, timestamps_and_values):
        """Insert points for a given metric.

        Args:
          metric_name: A graphite-like metric name (like "my.own.metric")
          timestamps_and_values: An iterable of (timestamp in seconds, values as double)
        """
        assert self.is_connected, "connect() wasn't called"
        statements_and_args = [
            self._make_insert_points(metric_name, t, v)
            for t, v in timestamps_and_values
        ]
        c_concurrent.execute_concurrent(
            self.__session, statements_and_args, concurrency=self.__executor_threads * 50)

    def drop_all_metrics(self):
        """Delete all metrics from the database."""
        assert self.is_connected, "connect() wasn't called"
        self.__session.execute("TRUNCATE metrics;")
        self.__session.execute("TRUNCATE raw;")

    @staticmethod
    def _round_down(timestamp, precision):
        return int(timestamp) // precision * precision

    def _make_all_points_selects(self, metric_name, time_start_ms, time_end_ms):
        # We fetch with ms precision, even though we only store with second precision.
        first_row = self._round_down(time_start_ms, self._ROW_SIZE_MS)
        last_row = self._round_down(time_end_ms, self._ROW_SIZE_MS)
        res = []
        # xrange(a,b) does not contain be, so we use last_row+1
        for row_start_ms in xrange(first_row, last_row + 1, self._ROW_SIZE_MS):
            row_min_offset = -1  # Selects all
            row_max_offset = self._ROW_SIZE_MS + 1   # Selects all
            if row_start_ms == first_row:
                row_min_offset = time_start_ms - row_start_ms
            if row_start_ms == last_row:
                row_max_offset = time_end_ms - row_start_ms
            statement = (self.__select_statement,
                         (metric_name, row_start_ms, row_min_offset, row_max_offset))
            res.append(statement)
        return res

    def update_metric(self, metric_metadata):
        """Update a metric definition from a MetricMetadata."""
        components = self._components_from_name(metric_metadata.name)
        empty_components = _COMPONENTS_MAX_LEN - len(components)
        if empty_components > 0:
            components += [None] * empty_components
        statement_args = [metric_metadata.name, metric_metadata.as_string_dict()] + components
        self.__session.execute(self.__insert_metrics_statement, statement_args)

    @staticmethod
    def median_aggregator(coverage, points):
        """The default aggregator for fetch(), returns the median of points."""
        if points:
            return statistics.median(points)
        else:
            return None

    def get_metric(self, metric_name):
        """Return a MetricMetadata for this metric_name, None if no such metric."""
        result = list(self.__session.execute(self.__select_metric_statement, (metric_name, )))
        if not result:
            return None
        name = result[0][0]
        config = result[0][1]
        config["name"] = name
        return MetricMetadata.from_string_dict(config)

    # TODO: handle ranges and the like
    # http://graphite.readthedocs.io/en/latest/render_api.html#paths-and-wildcards
    def glob_metric_names(self, metric_glob):
        """Return a sorted list of metric names matching this glob."""
        components = self._components_from_name(metric_glob)
        if len(components) > _COMPONENTS_MAX_LEN:
            msg = "Metric globs can have a maximum of %d dots" % _COMPONENTS_MAX_LEN - 2
            raise InvalidGlobError(msg)

        where = [
            "component_%d = %s" % (n, c_encoder.cql_quote(s))
            for n, s in enumerate(components)
            if s != "*"
        ]
        query = " ".join([
            "SELECT name FROM metrics WHERE",
            " AND ".join(where),
            "LIMIT %d ALLOW FILTERING;" % (self.MAX_METRIC_PER_GLOB + 1),
        ])
        try:
            metrics_names = [r.name for r in self.__session.execute(query)]
        except Exception as e:
            raise RetryableCassandraError(e)
        if len(metrics_names) > self.MAX_METRIC_PER_GLOB:
            msg = "%s yields more than %d metrics" % (metric_glob, self.MAX_METRIC_PER_GLOB)
            raise TooManyMetrics(msg)
        metrics_names.sort()
        return metrics_names

    # TODO: Remove aggregator_func and perform aggregation server-side.
    def fetch_points(self, metric_name, time_start, time_end, step, aggregator_func=None):
        """Fetch points from time_start included to time_end excluded with a duration of step.

        connect() must have previously been called.

        Args:
          time_start: timestamp in second from the Epoch as an int, inclusive,
            must be a multiple of step
          time_end: timestamp in second from the Epoch as an int, exclusive,
            must be a multiple of step
          step: time delta in seconds as an int, must be > 0
          aggregator_func: (coverage, values...)->value called to aggregate
            values regarding a single step. defaults to self.median_aggregator().
            Args:
              coverage: the proportion of steps for which we have values, as a float
              values: a list of float
            Returns:
              either a float or None to reject the step

        Returns:
          a list of (timestamp, value) where timestamp indicates the value is about the
          range from timestamp included to timestamp+step excluded

        Raises:
          InvalidArgumentError: if time_start, time_end or step are not as per above
        """
        assert self.is_connected, "connect() wasn't called"
        self._fetch_points_validate_args(metric_name, time_start, time_end, step, aggregator_func)
        aggregator_func = aggregator_func or self.median_aggregator
        step_ms = int(step) * 1000
        time_start_ms = int(time_start) * 1000
        time_end_ms = int(time_end) * 1000
        time_start_ms = max(time_end_ms - self._MAX_QUERY_RANGE_MS, time_start_ms)
        statements_and_args = self._make_all_points_selects(metric_name, time_start_ms, time_end_ms)
        query_results = c_concurrent.execute_concurrent(
            self.__session, statements_and_args, concurrency=self.__executor_threads * 50,
            results_generator=True,
        )

        first_exc = None
        current_points = []
        current_timestamp_ms = None
        res = []

        def add_current_points_to_res():
            aggregate = aggregator_func(len(current_points)/step, current_points)
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
                    add_current_points_to_res()
                    current_timestamp_ms = timestamp_ms
                    del current_points[:]

                current_points.append(row[2])

        if first_exc:
            raise RetryableCassandraError(first_exc)

        if current_timestamp_ms is not None:
            add_current_points_to_res()
        return res

    @property
    def is_connected(self):
        """Return true if connect() has been called since last shutdown()."""
        return bool(self.__cluster)

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
