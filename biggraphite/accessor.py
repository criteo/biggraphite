#!/usr/bin/env python
from __future__ import print_function

import cassandra
from cassandra import cluster as c_cluster
from cassandra import concurrent as c_concurrent
import statistics


class Error(Exception):
    """Base class for all exceptions from this module."""


class CassandraError(Error):
    """Fatal errors accessing Cassandra."""


class RetryableCassandraError(Error):
    """Errors accessing Cassandra that could succeed if retried."""


class InvalidArgumentError(Error):
    """Callee did not follow requirements on the arguments."""

_SETUP_CQL = [
    "CREATE TABLE raw ("
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
    "};",
]


class Accessor(object):
    """Provides Read/Write accessors to BigGraphite.

    It is safe to fork() or start new process until connect() has been called.
    It is not safe to share a given accessor across threads.
    """

    _ROW_SIZE_MS = 3600 * 1000
    _MAX_QUERY_RANGE_MS = 365 * 24 * _ROW_SIZE_MS

    def __init__(self, keyspace, contact_points, port=9042, connections_per_process=4):
        self.keyspace = keyspace
        self.contact_points = contact_points
        self.port = port
        self.connections_per_process = connections_per_process
        self.cluster = None  # setup by connect()
        self.insert_statement = None  # setup by connect()
        self.select_statement = None  # setup by connect()
        self.session = None  # setup by connect()

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, _type, _value, _traceback):
        self.shutdown()
        return False

    def _upgrade_schema(self):
        # Currently no change, so only upgrade operation is to setup
        try:
            self.session.execute("SELECT metric FROM raw LIMIT 1;")
        except cassandra.InvalidRequest:
            for cql in _SETUP_CQL:
                self.session.execute(cql)

    def connect(self):
        if self.cluster:
            return
        try:
            self._connect()
        except Exception as exc:
            try:
                self.shutdown()
            except Error:
                self.cluster = None
            raise CassandraError(exc)

    def shutdown(self):
        if self.cluster:
            try:
                self.cluster.shutdown()
            except Exception as exc:
                raise CassandraError(exc)
            self.cluster = None

    def _connect(self, skip_schema_upgrade=False):
        self.cluster = c_cluster.Cluster(
            self.contact_points, self.port, executor_threads=self.connections_per_process)
        self.session = self.cluster.connect()
        self.session.set_keyspace(self.keyspace)
        if not skip_schema_upgrade:
            self._upgrade_schema()
        self.insert_statement = self.session.prepare(
            "INSERT INTO raw (metric, time_start_ms, time_offset_ms, value)"
            " VALUES (?, ?, ?, ?);")
        self.insert_statement.consistency_level = cassandra.ConsistencyLevel.ONE
        self.select_statement = self.session.prepare(
            "SELECT time_start_ms, time_offset_ms, value FROM raw"
            " WHERE metric=? AND time_start_ms=? "
            " AND time_offset_ms >= ? AND time_offset_ms < ? "
            " ORDER BY time_offset_ms;")

    def _make_insert(self, metric_name, timestamp, value):
        # We store only one point per second, though the on-disk structure allow for more.
        timestamp_ms = int(timestamp) * 1000
        time_offset_ms = timestamp_ms % self._ROW_SIZE_MS
        time_start_ms = timestamp_ms - time_offset_ms
        return (self.insert_statement, (metric_name, time_start_ms, time_offset_ms, value))

    def insert_points(self, metric_name, timestamps_and_values):
        assert self.cluster, "connect() wasn't called"
        statements_and_args = [
            self._make_insert(metric_name, t, v)
            for t, v in timestamps_and_values
        ]
        c_concurrent.execute_concurrent(
            self.session, statements_and_args, concurrency=self.connections_per_process * 50)

    def clear_all_points(self):
        assert self.cluster, "connect() wasn't called"
        self.session.execute("TRUNCATE raw;")

    @staticmethod
    def _round_down(timestamp, precision):
        return int(timestamp) // precision * precision

    def _make_all_selects(self, metric_name, time_start_ms, time_end_ms):
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
            statement = (self.select_statement,
                         (metric_name, row_start_ms, row_min_offset, row_max_offset))
            res.append(statement)
        return res

    def get_aggregator(self, metric_name):
        # TODO: this is a shim that always give the median
        # should be avg, median, sum...
        # pylint: disable=no-self-use
        # pylint: disable=unused-argument
        return statistics.median

    def fetch_points(self, metric_name, time_start, time_end, step):
        """Fetch points from time_start included to time_end excluded with a duration of step.

        connect() must have previously been called.

        Args:
          time_start: timestamp in second from the Epoch as an int, inclusive,
            must be a multiple of step
          time_end: timestamp in second from the Epoch as an int, exclusive,
            must be a multiple of step
          step: time delta in seconds as an int, must be > 0

        Returns:
          a list of (timestamp, value) where timestamp indicates the value is about the
          range from timestamp included to timestamp+step excluded

        Raises:
          InvalidArgumentError: if time_start, time_end or step are not as per above
        """
        assert self.cluster, "connect() wasn't called"
        if step < 1:
            raise InvalidArgumentError("step (%d) is not positive" % step)
        if time_start % step or time_start < 0:
            raise InvalidArgumentError("time_start (%d) is not a multiple of step (%d)"
                                       % (time_start, step))
        if time_end % step or time_end < 0:
            raise InvalidArgumentError("time_end (%d) is not a multiple of step (%d)"
                                       % (time_end, step))

        aggregator_func = self.get_aggregator(metric_name)
        step_ms = int(step) * 1000
        time_start_ms = int(time_start) * 1000
        time_end_ms = int(time_end) * 1000
        time_start_ms = max(time_end_ms - self._MAX_QUERY_RANGE_MS, time_start_ms)
        statements_and_args = self._make_all_selects(metric_name, time_start_ms, time_end_ms)
        query_results = c_concurrent.execute_concurrent(
            self.session, statements_and_args, concurrency=self.connections_per_process * 50,
            results_generator=True,
        )

        first_exc = None
        current_points = []
        current_timestamp_ms = None

        res = []
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
                    if current_points:
                        res.append((current_timestamp_ms / 1000.0, aggregator_func(current_points)))
                    current_timestamp_ms = timestamp_ms
                    del current_points[:]

                current_points.append(row[2])

        if first_exc:
            raise RetryableCassandraError(first_exc)

        if current_timestamp_ms is not None and current_points:
            res.append((current_timestamp_ms / 1000.0, aggregator_func(current_points)))
        return res

    @property
    def is_connected(self):
        """Return true if connect() has been called since last shutdown()."""
        return bool(self.cluster)
