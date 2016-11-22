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

import itertools
import logging
import uuid
import os
from os import path as os_path
import multiprocessing
from datetime import datetime

import cassandra
from cassandra import murmur3
from cassandra import cluster as c_cluster
from cassandra import concurrent as c_concurrent
from cassandra import encoder as c_encoder
from cassandra import query as c_query
from cassandra import policies as c_policies
from cassandra.io import asyncorereactor as c_asyncorereactor

from biggraphite import accessor as bg_accessor
from biggraphite import glob_utils as bg_glob
from biggraphite.drivers import _downsampling
from biggraphite.drivers import _delayed_writer
from biggraphite.drivers import _utils

log = logging.getLogger(__name__)

# Round the row size to 1000 seconds
_ROW_SIZE_PRECISION_MS = 1000 * 1000

DEFAULT_KEYSPACE = "biggraphite"
DEFAULT_CONTACT_POINTS = "127.0.0.1"
DEFAULT_PORT = 9042
DEFAULT_TIMEOUT = 10.0
DEFAULT_CONNECTIONS = 4
# Disable compression per default as this is clearly useless for writes and
# reads do not generate that much traffic.
DEFAULT_COMPRESSION = False
# Current value is based on Cassandra page settings, so that everything fits in a single
# reply with default settings.
# TODO: Mesure actual number of metrics for existing queries and estimate a more
# reasonable limit, also consider other engines.
DEFAULT_MAX_METRICS_PER_GLOB = 5000
DEFAULT_TRACE = False
DEFAULT_BULKIMPORT = False
DEFAULT_MAX_WILDCARDS_FOR_GLOBSTAR = 32

OPTIONS = {
    "keyspace": str,
    "contact_points": _utils.list_from_str,
    "timeout": float,
    "connections": int,
    "compression": _utils.bool_from_str,
    "max_metrics_per_glob": int,
    "trace": bool,
    "bulkimport": bool,
}


def add_argparse_arguments(parser):
    """Add Cassandra arguments to an argparse parser."""
    parser.add_argument(
        "--cassandra_keyspace", metavar="NAME",
        help="Cassandra keyspace.",
        default=DEFAULT_KEYSPACE)
    parser.add_argument(
        "--cassandra_contact_points", metavar="HOST", nargs="+",
        help="Hosts used for discovery.",
        default=DEFAULT_CONTACT_POINTS)
    parser.add_argument(
        "--cassandra_port", metavar="PORT", type=int,
        help="The native port to connect to.",
        default=DEFAULT_PORT)
    parser.add_argument(
        "--cassandra_connections", metavar="N", type=int,
        help="Number of connections per Cassandra host per process.",
        default=DEFAULT_CONNECTIONS)
    parser.add_argument(
        "--cassandra_timeout", metavar="TIMEOUT", type=int,
        help="Cassandra query timeout in seconds.",
        default=DEFAULT_TIMEOUT)
    parser.add_argument(
        "--cassandra_compression", metavar="COMPRESSION", type=str,
        help="Cassandra network compression.",
        default=DEFAULT_COMPRESSION)
    parser.add_argument(
        "--cassandra_max_metrics_per_glob",
        help="Maximum number of metrics returned for a glob query.",
        default=DEFAULT_MAX_METRICS_PER_GLOB)
    parser.add_argument(
        "--cassandra_max_queries_for_globstar", type=int,
        help="Maximum number of wildcards (sub-queries) for globstar queries.",
        default=DEFAULT_MAX_WILDCARDS_FOR_GLOBSTAR)
    parser.add_argument(
        "--cassandra_trace",
        help="Enable query traces",
        default=DEFAULT_TRACE,
        action="store_true")
    parser.add_argument(
        "--cassandra_bulkimport", action="store_true",
        help="Generate files needed for bulkimport.")


MINUTE = 60
HOUR = 60 * MINUTE
DAY = 24 * HOUR
GLOBSTAR = "**"


class Error(bg_accessor.Error):
    """Base class for all exceptions from this module."""


class CassandraError(Error):
    """Fatal errors accessing Cassandra."""


class RetryableCassandraError(CassandraError, bg_accessor.RetryableError):
    """Errors accessing Cassandra that could succeed if retried."""


class NotConnectedError(CassandraError):
    """Fatal errors accessing Cassandra because the Accessor is not connected."""


class TooManyMetrics(CassandraError):
    """A name glob yielded more than MAX_METRIC_PER_GLOB metrics."""


class InvalidArgumentError(Error, bg_accessor.InvalidArgumentError):
    """Callee did not follow requirements on the arguments."""


class InvalidGlobError(InvalidArgumentError):
    """The provided glob is invalid."""


# TODO(c.chary): convert some of these to options, but make sure
# they are stored in the database an loaded automatically from
# here.

# CONSISTENCY PARAMETERS
# ======================
# Currently these are explicitely set to the defaults.
_META_WRITE_CONSISTENCY = cassandra.ConsistencyLevel.ONE
_META_READ_CONSISTENCY = cassandra.ConsistencyLevel.ONE

_DATA_WRITE_CONSISTENCY = cassandra.ConsistencyLevel.ONE
_DATA_READ_CONSISTENCY = cassandra.ConsistencyLevel.ONE


# HEURISTIC PARAMETERS
# ====================
# The following few constants are heuristics that are used to tune
# the datatable.
# We expect timestamp T to be written at T +/- _OUT_OF_ORDER_S
# As result we delay expiry and compaction by that much time
_OUT_OF_ORDER_S = 15 * MINUTE
# We expect to use this >>1440 points per read(~24h with a resolution of one minute).
# We round it up to a nicer value.
_EXPECTED_POINTS_PER_READ = 2000
# The API has a resolution of 1 sec. We don't want partitions to contain
# less than 6 hour of data (= 21600 points in the worst case).
_MIN_PARTITION_SIZE_MS = 6 * HOUR
# We also don't want partitions to be too big. The official limit is 100k.
_MAX_PARTITION_SIZE = 25000
# As we disable commit log, we flush memory data to disk every so
# often to make sure they are persisted.
_FLUSH_MEMORY_EVERY_S = 15 * MINUTE
# Can one of "DateTieredCompactionStrategy" or "TimeWindowCompactionStrategy".
# Support for TWCS is still experimental and require Cassandra >=3.8.
_COMPACTION_STRATEGY = "DateTieredCompactionStrategy"

_COMPONENTS_MAX_LEN = 64
_LAST_COMPONENT = "__END__"
_METADATA_CREATION_CQL_PATH_COMPONENTS = ", ".join(
    "component_%d text" % n for n in range(_COMPONENTS_MAX_LEN)
)

_METADATA_TOUCH_TTL_SEC = 3 * DAY

_METADATA_CREATION_CQL_METRICS_METADATA = str(
    "CREATE TABLE IF NOT EXISTS \"%(keyspace)s\".metrics_metadata ("
    "  name text,"
    "  updated_on  timestamp,"
    "  id uuid,"
    "  config map<text, text>,"
    "  PRIMARY KEY ((name))"
    ");"
)
_METADATA_CREATION_CQL_METRICS_METADATA_UPDATED_ON_INDEX = [
    "CREATE CUSTOM INDEX IF NOT EXISTS ON \"%%(keyspace)s\".%(table)s (updated_on)"
    "  USING 'org.apache.cassandra.index.sasi.SASIIndex'"
    "  WITH OPTIONS = {"
    "    'mode': 'SPARSE'"
    "  };" % {"table": "metrics_metadata"},
]

_METADATA_CREATION_CQL_METRICS = str(
    "CREATE TABLE IF NOT EXISTS \"%(keyspace)s\".metrics ("
    "  name text,"
    "  parent text,"
    "  " + _METADATA_CREATION_CQL_PATH_COMPONENTS + ","
    "  PRIMARY KEY (name)"
    ");"
)
_METADATA_CREATION_CQL_DIRECTORIES = str(
    "CREATE TABLE IF NOT EXISTS \"%(keyspace)s\".directories ("
    "  name text,"
    "  parent text,"
    "  " + _METADATA_CREATION_CQL_PATH_COMPONENTS + ","
    "  PRIMARY KEY (name)"
    ");"
)
_METADATA_CREATION_CQL_PARENT_INDEXES = [
    "CREATE CUSTOM INDEX IF NOT EXISTS ON \"%%(keyspace)s\".%(table)s (parent)"
    "  USING 'org.apache.cassandra.index.sasi.SASIIndex'"
    "  WITH OPTIONS = {"
    "    'analyzer_class': 'org.apache.cassandra.index.sasi.analyzer.NonTokenizingAnalyzer',"
    "    'case_sensitive': 'true'"
    "  };" % {"table": t}
    for t in ('metrics', 'directories')
]
_METADATA_CREATION_CQL_ID_INDEXES = [
    "CREATE CUSTOM INDEX IF NOT EXISTS ON \"%%(keyspace)s\".%(table)s (id)"
    "  USING 'org.apache.cassandra.index.sasi.SASIIndex'"
    "  WITH OPTIONS = {"
    "    'mode': 'SPARSE'"
    "  };" % {"table": "metrics_metadata"},
]
_METADATA_CREATION_CQL_PATH_INDEXES = [
    "CREATE CUSTOM INDEX IF NOT EXISTS ON \"%%(keyspace)s\".%(table)s (component_%(component)d)"
    "  USING 'org.apache.cassandra.index.sasi.SASIIndex'"
    "  WITH OPTIONS = {"
    "    'analyzer_class': 'org.apache.cassandra.index.sasi.analyzer.NonTokenizingAnalyzer',"
    "    'case_sensitive': 'true'"
    "  };" % {"table": t, "component": n}
    for t in ('metrics', 'directories')
    for n in range(_COMPONENTS_MAX_LEN)
]
_METADATA_CREATION_CQL = ([
    _METADATA_CREATION_CQL_METRICS,
    _METADATA_CREATION_CQL_DIRECTORIES,
    _METADATA_CREATION_CQL_METRICS_METADATA,
] + _METADATA_CREATION_CQL_PATH_INDEXES
  + _METADATA_CREATION_CQL_PARENT_INDEXES
  + _METADATA_CREATION_CQL_ID_INDEXES
  + _METADATA_CREATION_CQL_METRICS_METADATA_UPDATED_ON_INDEX
)


_DATAPOINTS_CREATION_CQL_TEMPLATE = str(
    "CREATE TABLE IF NOT EXISTS %(table)s ("
    "  metric uuid,"           # Metric UUID.
    "  time_start_ms bigint,"  # Lower bound for this row.
    "  offset smallint,"       # time_start_ms + offset * precision = timestamp
    "  value double,"          # Value for the point.
    "  count int,"             # If value is sum, divide by count to get the avg.
    "  PRIMARY KEY ((metric, time_start_ms), offset)"
    ")"
    "  WITH CLUSTERING ORDER BY (offset DESC)"
    "  AND default_time_to_live = %(default_time_to_live)d"
    "  AND memtable_flush_period_in_ms = %(memtable_flush_period_in_ms)d"
    "  AND comment = '%(comment)s'"
    "  AND compaction = {"
    "    'class': '%(compaction_strategy)s',"
    "    'timestamp_resolution': 'MICROSECONDS',"
    "    %(compaction_options)s"
    "  };"
)

_DATAPOINTS_CREATION_CQL_CS_TEMPLATE = {
    "DateTieredCompactionStrategy":  str(
        "    'base_time_seconds': '%(base_time_seconds)d',"
        "    'max_window_size_seconds': %(max_window_size_seconds)d"
    ),
    "TimeWindowCompactionStrategy": str(
        "    'compaction_window_unit': '%(compaction_window_unit)s',"
        "    'compaction_window_size': %(compaction_window_size)d"
    )
}


def _row_size_ms(stage):
    """Number of milliseconds to put in one Cassandra row.

    Args:
      stage: The stage the table stores.

    Returns:
      An integer, the duration in milliseconds.
    """
    row_size_ms = min(
        stage.precision_ms * _MAX_PARTITION_SIZE,
        max(
            stage.precision_ms * _EXPECTED_POINTS_PER_READ,
            _MIN_PARTITION_SIZE_MS
        )
    )
    return bg_accessor.round_up(row_size_ms, _ROW_SIZE_PRECISION_MS)


class _CappedConnection(c_asyncorereactor.AsyncoreConnection):
    """A connection with a cap on the number of in-flight requests per host."""

    # 300 is the minimum with protocol version 3, default is 65536
    max_in_flight = 500


class _CountDown(_utils.CountDown):
    """Cassandra specific version of CountDown."""

    def on_cassandra_result(self, result):
        self.on_result(result)

    def on_cassandra_failure(self, exc):
        self.on_failure(exc)


class _LazyPreparedStatements(object):
    """On demand factory of prepared statements and tables.

    As per design (CASSANDRA_DESIGN.md) we have one table per retention stage.
    This creates tables and corresponding prepared statements once they are needed.

    When bulkimport is True this class will instead write the files necessary to
    bulkimport data.
    """

    def __init__(self, session, keyspace, bulkimport=False):
        self._keyspace = keyspace
        self._session = session
        self._bulkimport = bulkimport
        self.__stage_to_insert = {}
        self.__stage_to_select = {}
        self.__data_files = {}

    def __bulkimport_filename(self, filename):
        current = multiprocessing.current_process()
        uid = str(current._identity[0]) if len(current._identity) else "0"
        filename = os_path.join("data", uid, filename)
        dirname = os_path.dirname(filename)
        if not os_path.exists(dirname):
            os.makedirs(dirname)
        return filename

    def _bulkimport_write_schema(self, stage, statement_str):
        filename = self.__bulkimport_filename(stage.as_string + ".cql")
        log.info("Writing schema for '%s' in '%s'" % (stage.as_string, filename))
        open(filename, "w").write(statement_str)

    def _bulkimport_write_datapoint(self, stage, args):
        stage_str = stage.as_string
        if stage_str not in self.__data_files:
            statement_str = self._create_datapoints_table_stmt(stage)
            self._bulkimport_write_schema(stage, statement_str)

            filename = self.__bulkimport_filename(stage.as_string + ".csv")
            log.info("Writing data for '%s' in '%s'" % (stage.as_string, filename))
            fp = open(filename, "w")
            self.__data_files[stage_str] = fp
        else:
            fp = self.__data_files[stage_str]
            fp.write(",".join([str(a) for a in args]) + "\n")

    def _create_datapoints_table_stmt(self, stage):
        # Time after which data expire.
        time_to_live = stage.duration + _OUT_OF_ORDER_S

        # Estimate the age of the oldest data we still expect to read.
        fresh_time = stage.precision * _EXPECTED_POINTS_PER_READ

        cs_template = _DATAPOINTS_CREATION_CQL_CS_TEMPLATE.get(_COMPACTION_STRATEGY)
        if not cs_template:
            raise InvalidArgumentError(
                "Unknown compaction strategy '%s'" % _COMPACTION_STRATEGY)
        cs_kwargs = {}

        if _COMPACTION_STRATEGY == "DateTieredCompactionStrategy":
            # Time it takes to receive a step
            arrival_time = stage.precision + _OUT_OF_ORDER_S

            # See http://www.datastax.com/dev/blog/datetieredcompactionstrategy
            #  - If too small: Reads need to touch many sstables
            #  - If too big: We pay compaction overhead for data that are
            #    never accessed anymore and get huge sstables
            # We set a minimum of arrival_time so that data are in order
            max_window_size_seconds = max(fresh_time, arrival_time + 1)

            cs_kwargs["base_time_seconds"] = arrival_time
            cs_kwargs["max_window_size_seconds"] = max_window_size_seconds
        elif _COMPACTION_STRATEGY == "TimeWindowCompactionStrategy":
            # TODO(c.chary): Tweak this once we have an actual 3.9 setup.

            window_size = max(
                # Documentation says that we should no more than 50 buckets.
                time_to_live / 50,
                # But we don't want multiple sstables per hour.
                HOUR,
                # Also try to optimize for reads
                fresh_time
            )

            # Make it readable.
            if window_size > DAY:
                unit = 'DAYS'
                window_size /= DAY
            else:
                unit = 'HOURS'
                window_size /= HOUR

            cs_kwargs["compaction_window_unit"] = unit
            cs_kwargs["compaction_window_size"] = max(1, window_size)

        compaction_options = cs_template % cs_kwargs
        kwargs = {
            "table": self._get_table_name(stage),
            "default_time_to_live": time_to_live,
            "memtable_flush_period_in_ms": _FLUSH_MEMORY_EVERY_S * 1000,
            "comment": "{\"created_by\": \"biggraphite\", \"schema_version\": 0}",
            "compaction_strategy": _COMPACTION_STRATEGY,
            "compaction_options": compaction_options,
        }

        return _DATAPOINTS_CREATION_CQL_TEMPLATE % kwargs

    def _create_datapoints_table(self, stage):
        # The statement is idempotent
        statement_str = self._create_datapoints_table_stmt(stage)
        self._session.execute(statement_str)

    def _get_table_name(self, stage):
        return "\"{}\".\"datapoints_{}p_{}s\"".format(
            self._keyspace, stage.points, stage.precision)

    def prepare_insert(self, stage, metric_id, time_start_ms, offset, value, count):
        statement = self.__stage_to_insert.get(stage)
        args = (metric_id, time_start_ms, offset, value, count)

        if self._bulkimport:
            self._bulkimport_write_datapoint(stage, args)
            return None, args

        if statement:
            return statement, args

        self._create_datapoints_table(stage)
        statement_str = (
            "INSERT INTO %(table)s"
            " (metric, time_start_ms, offset, value, count)"
            " VALUES (?, ?, ?, ?, ?);"
        ) % {"table": self._get_table_name(stage)}
        statement = self._session.prepare(statement_str)
        statement.consistency_level = _DATA_WRITE_CONSISTENCY
        self.__stage_to_insert[stage] = statement
        return statement, args

    def prepare_select(self, stage, metric_id, row_start_ms, row_min_offset, row_max_offset):
        statement = self.__stage_to_select.get(stage)
        limit = (row_max_offset - row_min_offset)
        args = (metric_id, row_start_ms, row_min_offset, row_max_offset, limit)
        if statement:
            return statement, args

        self._create_datapoints_table(stage)
        statement_str = (
            "SELECT time_start_ms, offset, value, count FROM %(table)s"
            " WHERE metric=? AND time_start_ms=?"
            " AND offset >= ? AND offset < ? "
            " ORDER BY offset"
            " LIMIT ?;"
        ) % {"table": self._get_table_name(stage)}
        statement = self._session.prepare(statement_str)
        statement.consistency_level = _DATA_READ_CONSISTENCY
        self.__stage_to_select[stage] = statement
        return statement, args


class _CassandraAccessor(bg_accessor.Accessor):
    """Provides Read/Write accessors to Cassandra.

    Please refer to bg_accessor.Accessor.
    """

    _UUID_NAMESPACE = uuid.UUID('{00000000-1111-2222-3333-444444444444}')

    def __init__(self,
                 keyspace='biggraphite',
                 contact_points=DEFAULT_CONTACT_POINTS,
                 port=DEFAULT_PORT,
                 connections=DEFAULT_CONNECTIONS,
                 timeout=DEFAULT_TIMEOUT,
                 compression=DEFAULT_COMPRESSION,
                 max_metrics_per_glob=DEFAULT_MAX_METRICS_PER_GLOB,
                 max_wildcards_for_globstar=DEFAULT_MAX_WILDCARDS_FOR_GLOBSTAR,
                 trace=DEFAULT_TRACE,
                 bulkimport=DEFAULT_BULKIMPORT):
        """Record parameters needed to connect.

        Args:
          keyspace: Base names of Cassandra keyspaces dedicated to BigGraphite.
          contact_points: list of strings, the hostnames or IP to use to discover Cassandra.
          port: The port to connect to, as an int.
          connections: How many worker threads to use.
          timeout: Default timeout for operations in seconds.
          compression: One of False, True, "lz4", "snappy"
          max_metrics_per_glob: int, Maximum number of metrics per glob.
          trace: bool, Enabling query tracing.
          bulkimport: bool, Configure the accessor to generate files necessary for
            bulk import.
        """
        backend_name = "cassandra:" + keyspace
        super(_CassandraAccessor, self).__init__(backend_name)
        self.keyspace = keyspace
        self.keyspace_metadata = keyspace + "_metadata"
        self.contact_points = contact_points
        self.port = port
        self.max_metrics_per_glob = max_metrics_per_glob
        self.max_wildcards_for_globstar = max_wildcards_for_globstar
        self.__connections = connections
        self.__compression = compression
        self.__trace = trace
        self.__bulkimport = bulkimport
        self.__metadata_touch_ttl_sec = _METADATA_TOUCH_TTL_SEC
        # For some reason this isn't enabled yet for pypy, even if it seems to
        # be working properly.
        # See https://github.com/datastax/python-driver/blob/master/cassandra/cluster.py#L188
        self.__load_balancing_policy = (
            c_policies.TokenAwarePolicy(c_policies.DCAwareRoundRobinPolicy()))
        self.__downsampler = _downsampling.Downsampler()
        self.__delayed_writer = _delayed_writer.DelayedWriter(self)
        self.__cluster = None  # setup by connect()
        self.__lazy_statements = None  # setup by connect()
        self.__timeout = timeout
        self.__insert_metric_statement = None  # setup by connect()
        self.__select_metric_metadata_statement = None  # setup by connect()
        self.__update_metric_metadata_statement = None  # setup by connect()
        self.__touch_metrics_metadata_statement = None  # setup by connect()
        self.__session = None  # setup by connect()
        self.__glob_parser = bg_glob.GraphiteGlobParser()

    def connect(self, skip_schema_upgrade=False):
        """See bg_accessor.Accessor."""
        super(_CassandraAccessor, self).connect(skip_schema_upgrade=skip_schema_upgrade)
        self.__cluster = c_cluster.Cluster(
            self.contact_points, self.port,
            executor_threads=self.__connections,
            compression=self.__compression,
            load_balancing_policy=self.__load_balancing_policy,
            # TODO(c.chary): export these metrics somewhere.
            metrics_enabled=True,
        )
        self.__cluster.connection_class = _CappedConnection  # Limits in flight requests
        self.__session = self.__cluster.connect()
        self.__session.row_factory = c_query.tuple_factory  # Saves 2% CPU
        if self.__timeout:
            self.__session.default_timeout = self.__timeout
        if not skip_schema_upgrade:
            self._upgrade_schema()

        self.__lazy_statements = _LazyPreparedStatements(
            self.__session, self.keyspace, self.__bulkimport)

        # Metadata (metrics and directories)
        components_names = ", ".join("component_%d" % n for n in range(_COMPONENTS_MAX_LEN))
        components_marks = ", ".join("?" for n in range(_COMPONENTS_MAX_LEN))
        self.__insert_metric_statement = self.__session.prepare(
            "INSERT INTO \"%s\".metrics (name, parent, %s) VALUES (?, ?, %s);"
            % (self.keyspace_metadata, components_names, components_marks)
        )
        self.__insert_metric_statement.consistency_level = _META_WRITE_CONSISTENCY
        self.__insert_directory_statement = self.__session.prepare(
            "INSERT INTO \"%s\".directories (name, parent, %s) VALUES (?, ?, %s) IF NOT EXISTS;"
            % (self.keyspace_metadata, components_names, components_marks)
        )
        self.__insert_directory_statement.consistency_level = _META_WRITE_CONSISTENCY
        # We do not set the serial_consistency, it defautls to SERIAL.
        self.__select_metric_metadata_statement = self.__session.prepare(
            "SELECT id, config, updated_on FROM \"%s\".metrics_metadata WHERE name = ?;"
            % self.keyspace_metadata
        )
        self.__select_metric_metadata_statement.consistency_level = _META_READ_CONSISTENCY
        self.__update_metric_metadata_statement = self.__session.prepare(
            "UPDATE \"%s\".metrics_metadata SET config=?, updated_on=toTimestamp(now())"
            " WHERE name=?;" % self.keyspace_metadata
        )
        self.__update_metric_metadata_statement.consistency_level = _META_WRITE_CONSISTENCY
        self.__touch_metrics_metadata_statement = self.__session.prepare(
            "UPDATE \"%s\".metrics_metadata SET updated_on=toTimestamp(now())"
            " WHERE name=?;" % self.keyspace_metadata
        )
        self.__touch_metrics_metadata_statement.consistency_level = _META_WRITE_CONSISTENCY
        self.__insert_metrics_metadata_statement = self.__session.prepare(
            "INSERT INTO \"%s\".metrics_metadata (name, updated_on, id, config)"
            " VALUES (?, toTimestamp(now()), ?, ?);" % self.keyspace_metadata
        )
        self.__insert_metrics_metadata_statement.consistency_level = _META_WRITE_CONSISTENCY

        self.is_connected = True

    def _execute(self, *args, **kwargs):
        """Wrapper for __session.execute_async()."""
        if self.__bulkimport:
            return []

        if self.__trace:
            kwargs["trace"] = True

        log.debug(' '.join([str(arg) for arg in args]))
        result = self.__session.execute(*args, **kwargs)

        if self.__trace:
            trace = result.get_query_trace()
            for e in trace.events:
                log.debug("%s: %s" % (e.source_elapsed, str(e)))
        return result

    def _execute_async(self, *args, **kwargs):
        """Wrapper for __session.execute_async()."""
        if self.__bulkimport:
            class _FakeFuture(object):
                def add_callbacks(self, on_result, on_failure):
                    on_result(None)
            return _FakeFuture()

        if self.__trace:
            kwargs["trace"] = True

        log.debug(' '.join([str(arg) for arg in args]))
        future = self.__session.execute_async(*args, **kwargs)

        if self.__trace:
            trace = future.get_query_trace()
            for e in trace.events:
                log.debug(e.source_elapsed, e.description)
        return future

    def _execute_concurrent(self, statements_and_parameters, **kwargs):
        """Wrapper for concurrent.execute_concurrent()."""
        if self.__bulkimport:
            return []

        log.debug(statements_and_parameters)

        if not self.__trace:
            return c_concurrent.execute_concurrent(
                self.__session, statements_and_parameters, **kwargs)

        query_results = []
        for statement, params in statements_and_parameters:
            try:
                result = self._execute(statement, params, trace=True)
                success = True
            except Exception as e:
                result = e
                success = False
            query_results.append((success, result))
        return query_results

    def make_metric(self, name, metadata):
        """See bg_accessor.Accessor."""
        # Cleanup name (avoid double dots)
        name = ".".join(self._components_from_name(name)[:-1])
        encoded_name = bg_accessor.encode_metric_name(name)
        id = uuid.uuid5(self._UUID_NAMESPACE, encoded_name)
        return bg_accessor.Metric(name, id, metadata)

    def create_metric(self, metric):
        """See bg_accessor.Accessor."""
        super(_CassandraAccessor, self).create_metric(metric)

        if self.__bulkimport:
            return

        components = self._components_from_name(metric.name)
        queries = []

        # Check if parent dir exists. This is one round-trip but worthwile since
        # otherwise creating each parent directory requires a round-trip and the
        # vast majority of metrics have siblings.
        parent_dir = metric.name.rpartition(".")[0]
        if parent_dir and not self.glob_directory_names(parent_dir):
            queries.extend(self._create_parent_dirs_queries(components))

        # Finally, create the metric
        padding_len = _COMPONENTS_MAX_LEN - len(components)
        padding = [c_query.UNSET_VALUE] * padding_len
        metadata_dict = metric.metadata.as_string_dict()
        queries.append((
            self.__insert_metric_statement,
            [metric.name, parent_dir + "."] + components + padding,
        ))
        queries.append((
            self.__insert_metrics_metadata_statement,
            [metric.name, metric.id, metadata_dict],
        ))

        # We have to run queries in sequence as:
        #  - we want them to have IF NOT EXISTS ease the hotspot on root directories
        #  - we do not want directories or metrics without parents (not handled by callee)
        #  - batch queries cannot contain IF NOT EXISTS and involve multiple primary keys
        # We can still end up with empty directories, which will need a reaper job to clean them.
        for statement, args in queries:
            self._execute(statement, args)

    def update_metric(self, name, updated_metadata):
        """See bg_accessor.Accessor."""
        super(_CassandraAccessor, self).update_metric(name, updated_metadata)

        if not self.has_metric(name):
            raise InvalidArgumentError(
                "Unknown metric '%s'" % name)

        # Cleanup name (avoid double dots)
        name = ".".join(self._components_from_name(name)[:-1])

        encoded_metric_name = bg_accessor.encode_metric_name(name)
        metadata_dict = updated_metadata.as_string_dict()
        self._execute(self.__update_metric_metadata_statement, [metadata_dict, encoded_metric_name])

    def _create_parent_dirs_queries(self, components):
        queries = []
        path = []
        parent = ''
        # Incrementally construct parents, till we reach the leaf component.
        # -2 skips leaf (metric name) and _LAST_COMPONENT marker.
        for component in components[:-2]:
            path.append(component)
            name = '.'.join(path)
            path_components = path + [_LAST_COMPONENT]
            padding_len = _COMPONENTS_MAX_LEN - len(path_components)
            padding = [c_query.UNSET_VALUE] * padding_len
            queries.append((
                self.__insert_directory_statement,
                [name, parent + '.'] + path_components + padding,
            ))
            parent = name

        return queries

    @staticmethod
    def _components_from_name(metric_name):
        res = metric_name.split(".")
        res.append(_LAST_COMPONENT)
        return filter(None, res)

    def drop_all_metrics(self):
        """See bg_accessor.Accessor."""
        super(_CassandraAccessor, self).drop_all_metrics()
        for keyspace in self.keyspace, self.keyspace_metadata:
            statement_str = "SELECT table_name FROM system_schema.tables WHERE keyspace_name = %s;"
            tables = [r[0] for r in self._execute(statement_str, (keyspace, ))]
            for table in tables:
                self._execute("TRUNCATE \"%s\".\"%s\";" % (keyspace, table))

    def fetch_points(self, metric, time_start, time_end, stage):
        """See bg_accessor.Accessor."""
        super(_CassandraAccessor, self).fetch_points(
            metric, time_start, time_end, stage)

        log.debug(
            "fetch: [%s, start=%d, end=%d, stage=%s]",
            metric.name, time_start, time_end, stage)

        time_start_ms = int(time_start) * 1000
        time_end_ms = int(time_end) * 1000
        time_start_ms = max(time_end_ms - stage.duration_ms, time_start_ms)

        statements_and_args = self._fetch_points_make_selects(
            metric.id, time_start_ms, time_end_ms, stage)
        query_results = self._execute_concurrent(
            statements_and_args,
            results_generator=True,
        )
        return bg_accessor.PointGrouper(
            metric, time_start_ms, time_end_ms, stage, query_results
        )

    def _fetch_points_make_selects(self, metric_id, time_start_ms,
                                   time_end_ms, stage):
        # We fetch with ms precision, even though we only store with second
        # precision.
        row_size_ms_stage = _row_size_ms(stage)
        first_row = bg_accessor.round_down(time_start_ms, row_size_ms_stage)
        last_row = bg_accessor.round_down(time_end_ms, row_size_ms_stage)
        res = []
        # xrange(a,b) does not contain b, so we use last_row+1
        for row_start_ms in xrange(first_row, last_row + 1, row_size_ms_stage):
            # adjust min/max offsets to select everything
            row_min_offset_ms = 0
            row_max_offset_ms = row_size_ms_stage
            if row_start_ms == first_row:
                row_min_offset_ms = time_start_ms - row_start_ms
            if row_start_ms == last_row:
                row_max_offset_ms = time_end_ms - row_start_ms

            row_min_offset = stage.step_ms(row_min_offset_ms)
            row_max_offset = stage.step_ms(row_max_offset_ms)

            select = self.__lazy_statements.prepare_select(
                stage=stage,
                metric_id=metric_id,
                row_start_ms=row_start_ms,
                row_min_offset=row_min_offset,
                row_max_offset=row_max_offset,
            )
            res.append(select)

        return res

    def _select_metric(self, metric_name):
        """Fetch metric metadata."""
        encoded_metric_name = bg_accessor.encode_metric_name(metric_name)
        result = list(self._execute(
            self.__select_metric_metadata_statement, (encoded_metric_name, )))
        if not result:
            return None
        return result[0]

    def has_metric(self, metric_name):
        """See bg_accessor.Accessor."""
        super(_CassandraAccessor, self).has_metric(metric_name)
        metric_name = ".".join(self._components_from_name(metric_name)[:-1])
        metric_name = bg_accessor.encode_metric_name(metric_name)
        if not self._select_metric(metric_name):
            return False

        # Small trick here: we also check that the parent directory
        # exists because that's what we check to create the directory
        # hierarchy.
        parent_dir = metric_name.rpartition(".")[0]
        if parent_dir and not self.glob_directory_names(parent_dir):
            return False

        return True

    def __touch_metric_if_expired(self, updated_on, metric_name):
        if (datetime.today() - updated_on).total_seconds() >= self.__metadata_touch_ttl_sec:
            self.touch_metric(metric_name)

    def get_metric(self, metric_name):
        """See bg_accessor.Accessor."""
        super(_CassandraAccessor, self).get_metric(metric_name)
        metric_name = ".".join(self._components_from_name(metric_name)[:-1])
        metric_name = bg_accessor.encode_metric_name(metric_name)
        result = self._select_metric(metric_name)
        if not result:
            return None
        id = result[0]
        config = result[1]
        updated_on = result[2]
        self.__touch_metric_if_expired(updated_on, metric_name)

        metadata = bg_accessor.MetricMetadata.from_string_dict(config)
        return bg_accessor.Metric(metric_name, id, metadata)

    def glob_directory_names(self, glob):
        """Return a sorted list of metric directories matching this glob."""
        super(_CassandraAccessor, self).glob_directory_names(glob)
        return self.__glob_names("directories", glob)

    def glob_metric_names(self, glob):
        """Return a sorted list of metric names matching this glob."""
        super(_CassandraAccessor, self).glob_metric_names(glob)
        return self.__glob_names("metrics", glob)

    def __glob_names(self, table, glob):
        components = self.__glob_parser.parse(glob)
        if len(components) > _COMPONENTS_MAX_LEN:
            raise bg_accessor.InvalidGlobError(
                "Contains %d components, but we support %d at most" % (
                    len(components),
                    _COMPONENTS_MAX_LEN,
                )
            )

        queries = []
        globstar = bg_glob.Globstar()
        if globstar in components:
            # Handling more than one of these can cause combinatorial explosion.
            if components.count(globstar) > 1:
                raise bg_accessor.InvalidGlobError(
                    "Contains more than one globstar (**) operator"
                )

            # If the globstar operator is at the end of the pattern, then we can
            # find corresponding metrics with a prefix search;
            # otherwise, we have to generate incremental queries that go up to a
            # certain depth (_COMPONENTS_MAX_LEN - #components).
            gs_index = components.index(globstar)
            if gs_index == len(components) - 1:
                queries.append(
                    self.__build_select_names_query(
                        table, components[:gs_index], glob,
                    )
                )
            else:
                prefix = components[:gs_index]
                suffix = components[gs_index+1:] + [[_LAST_COMPONENT]]
                max_wildcards = min(self.max_wildcards_for_globstar,
                                    _COMPONENTS_MAX_LEN - len(components))
                for wildcards in range(1, max_wildcards):
                    gs_components = (prefix +
                                     ([[bg_glob.AnySequence()]] * wildcards) +
                                     suffix)
                    queries.append(
                        self.__build_select_names_query(
                            table, gs_components, glob,
                        )
                    )
        else:
            queries.append(
                self.__build_select_names_query(
                    table, components + [[_LAST_COMPONENT]], glob,
                )
            )

        metric_names = []
        try:
            for query in queries:
                statement = c_query.SimpleStatement(query)
                statement.consistency_level = _META_READ_CONSISTENCY
                for row in self._execute(query):
                    metric_names.append(row[0])
                if len(metric_names) > self.max_metrics_per_glob:
                    break
        except Exception as e:
            raise RetryableCassandraError(e)

        if len(metric_names) > self.max_metrics_per_glob:
            raise TooManyMetrics(
                "Query yields more than %d results" %
                (self.max_metrics_per_glob)
            )

        metric_names.sort()
        return metric_names

    def __build_select_names_query(self, table, components, glob):
        query_select = "SELECT name FROM \"%s\".\"%s\"" % (
            self.keyspace_metadata,
            table,
        )
        query_limit = "LIMIT %d" % (self.max_metrics_per_glob + 1)

        if len(components) == 0:
            return "%s %s;" % (query_select, query_limit)

        # If all components are constant values we can search by exact name.
        # If all but the last component are constant values we can search by
        # exact parent, in which case we may benefit from filtering the last
        # component by prefix when we have one. (Code refers to the previous-to
        # -last component because of the __END__ suffix we use).
        #
        # We are not using prefix search on the parent because it appears to be
        # too slow/costly at the moment (see #174 for details).
        if (
            components[-1] == [_LAST_COMPONENT] and  # Not a prefix globstar
            all(len(c) == 1 and isinstance(c[0], str) for c in components[:-2])
        ):
            last = components[-2]
            if len(last) == 1 and isinstance(last[0], str):
                return "%s WHERE name = %s %s;" % (
                    query_select,
                    c_encoder.cql_quote(glob),
                    query_limit,
                )
            else:
                if len(last) > 0 and isinstance(last[0], str):
                    prefix_filter = "AND component_%d LIKE %s" % (
                        len(components) - 1,
                        c_encoder.cql_quote(last[0] + '%'),
                    )
                    allow_filtering = "ALLOW FILTERING"
                else:
                    prefix_filter = ''
                    allow_filtering = ''

                parent = itertools.chain.from_iterable(components[:-2])
                parent = '.'.join(parent) + '.'
                return "%s WHERE parent = %s %s %s %s;" % (
                    query_select,
                    c_encoder.cql_quote(parent),
                    prefix_filter,
                    query_limit,
                    allow_filtering,
                )

        where_clauses = []
        for n, component in enumerate(components):
            if len(component) == 0:
                continue

            # We are currently using prefix indexes, so if we do not have a
            # prefix value (i.e. it is a wildcard), then the current component
            # cannot be constrained inside the request.
            value = component[0]
            if not isinstance(value, str):
                continue

            if len(component) == 1:
                op = '='
            else:
                op = "LIKE"
                value += '%'

            clause = "component_%d %s %s" % (n, op, c_encoder.cql_quote(value))
            where_clauses.append(clause)

        if len(where_clauses) == 0:
            return "%s %s;" % (query_select, query_limit)

        return "%s WHERE %s %s ALLOW FILTERING;" % (
            query_select,
            " AND ".join(where_clauses),
            query_limit
        )

    def flush(self):
        """Flush any internal buffers."""
        if self.__delayed_writer:
            self.__delayed_writer.flush()

    def insert_points_async(self, metric, datapoints, on_done=None):
        """See bg_accessor.Accessor."""
        super(_CassandraAccessor, self).insert_points_async(
            metric, datapoints, on_done)

        log.debug("insert: [%s, %s]", metric.name, datapoints)

        datapoints = self.__downsampler.feed(metric, datapoints)
        if self.__delayed_writer:
            datapoints = self.__delayed_writer.feed(metric, datapoints)
        return self.insert_downsampled_points_async(metric, datapoints, on_done)

    def insert_downsampled_points_async(self, metric, datapoints, on_done=None):
        """See bg_accessor.Accessor."""
        if not datapoints and on_done:
            on_done(None)
            return

        count_down = None
        if on_done:
            count_down = _CountDown(count=len(datapoints), on_zero=on_done)

        for timestamp, value, count, stage in datapoints:
            timestamp_ms = int(timestamp) * 1000
            time_offset_ms = timestamp_ms % _row_size_ms(stage)
            time_start_ms = timestamp_ms - time_offset_ms
            offset = stage.step_ms(time_offset_ms)

            statement, args = self.__lazy_statements.prepare_insert(
                stage=stage, metric_id=metric.id, time_start_ms=time_start_ms,
                offset=offset, value=value, count=count,
            )
            future = self._execute_async(query=statement, parameters=args)
            if count_down:
                future.add_callbacks(
                    count_down.on_cassandra_result,
                    count_down.on_cassandra_failure,
                )

    def repair(self, start_key=None, end_key=None, shard=0, nshards=1):
        """See bg_accessor.Accessor.

        Slight change for start_key and end_key, they are intrepreted as
        tokens directly.
        """
        super(_CassandraAccessor, self).repair()

        partitioner = self.__cluster.metadata.partitioner
        if partitioner != "org.apache.cassandra.dht.Murmur3Partitioner":
            log.warn("Partitioner '%s' not supported for repairs" % partitioner)
            return

        start_token = murmur3.INT64_MIN
        stop_token = murmur3.INT64_MAX
        batch_size = 1000

        if nshards > 1:
            tokens = murmur3.INT64_MAX - murmur3.INT64_MIN
            my_tokens = tokens / nshards
            start_token += my_tokens * shard
            stop_token = start_token + my_tokens

        if start_key is not None:
            start_token = int(start_key)
        if end_key is not None:
            end_key = int(end_key)

        # Step 1: Create missing parent directories.
        statement_str = (
            "SELECT name, parent, token(name) FROM \"%s\".directories "
            "WHERE token(name) > ? LIMIT %d;" %
            (self.keyspace_metadata, batch_size))
        statement = self.__session.prepare(statement_str)
        statement.consistency_level = cassandra.ConsistencyLevel.QUORUM
        statement.retry_policy = cassandra.policies.DowngradingConsistencyRetryPolicy
        statement.request_timeout = 60

        token = start_token
        while token < stop_token:
            args = (token,)
            result = self._execute(statement, args)

            if len(result.current_rows) == 0:
                break
            for row in result:
                name, parent, token = row
                parent_dir = name.rpartition(".")[0]
                if parent_dir and not self.glob_directory_names(parent_dir):
                    log.warning("Creating missing parent dir '%s'" % parent_dir)
                    components = self._components_from_name(name)
                    queries = self._create_parent_dirs_queries(components)
                    for i_statement, i_args in queries:
                        self._execute(i_statement, i_args)

                # TODO(c.chary): fix the parent field.

                token = token

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
            self.__cluster.refresh_schema_metadata()
        except cassandra.DriverException:
            log.exception("Failed to refresh metadata.")
            return

        keyspaces = self.__cluster.metadata.keyspaces.keys()
        for keyspace in [self.keyspace, self.keyspace_metadata]:
            if keyspace not in keyspaces:
                raise CassandraError("Missing keyspace '%s'." % keyspace)

        tables = self.__cluster.metadata.keyspaces[self.keyspace_metadata].tables
        mandatory_tables = ['metrics', 'directories', 'metrics_metadata']

        if reduce(lambda acc, val: acc and val in tables, mandatory_tables):
            return

        for cql in _METADATA_CREATION_CQL:
            self._execute(cql % {"keyspace": self.keyspace_metadata})

    def touch_metric(self, metric_name):
        """See the real Accessor for a description."""
        super(_CassandraAccessor, self).touch_metric(metric_name)

        if self.__bulkimport:
            return

        queries = []
        queries.append((
            self.__touch_metrics_metadata_statement,
            [metric_name],
        ))

        for statement, args in queries:
            self._execute(statement, args)

    def clean(self, cutoff=None):
        """See bg_accessor.Accessor.

        Args:
            cutoff: UNIX time in seconds. Rows older than it should be deleted.
        """
        super(_CassandraAccessor, self).clean(cutoff)

        if not cutoff:
            log.warn("You must specify a cutoff time for cleanup")
            return

        # timestamp format in Cassandra is in milliseconds
        cutoff = int(cutoff) * 1000
        log.info("Cleaning with cutoff time %d", cutoff)

        # statements
        select = self.__session.prepare(
            "SELECT name FROM \"%s\".metrics_metadata"
            " WHERE updated_on <= %d;" %
            (self.keyspace_metadata, cutoff))
        select.consistency_level = cassandra.ConsistencyLevel.LOCAL_QUORUM
        select.retry_policy = cassandra.policies.DowngradingConsistencyRetryPolicy
        select.request_timeout = 120

        delete = self.__session.prepare(
            "DELETE FROM \"%s\".metrics WHERE name = ? ;" %
            (self.keyspace_metadata))
        delete_metadata = self.__session.prepare(
            "DELETE FROM \"%s\".metrics_metadata WHERE name = ? ;" %
            (self.keyspace_metadata))

        result = self._execute(select)
        for row in result:
            log.info("Cleaning metric %s", row)
            self._execute(delete, row)  # cleanup metrics
            self._execute(delete_metadata, row)  # cleanup modification time


def build(*args, **kwargs):
    """Return a bg_accessor.Accessor using Casssandra.

    Args:
      keyspace: Base name of Cassandra keyspaces dedicated to BigGraphite.
      contact_points: list of strings, the hostnames or IP to use to discover Cassandra.
      port: The port to connect to, as an int.
      connections: How many worker threads to use.
    """
    return _CassandraAccessor(*args, **kwargs)
