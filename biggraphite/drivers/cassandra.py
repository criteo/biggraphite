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

import collections
import itertools
import json
import logging
import multiprocessing
import os
import random
import time
import uuid
from os import path as os_path
from distutils import version

import cassandra
from cassandra import murmur3
from cassandra import cluster as c_cluster
from cassandra import concurrent as c_concurrent
from cassandra import encoder as c_encoder
from cassandra import query as c_query
from cassandra import marshal as c_marshal

from biggraphite import accessor as bg_accessor
from biggraphite import glob_utils as bg_glob
from biggraphite.drivers import _downsampling
from biggraphite.drivers import _delayed_writer
from biggraphite.drivers import _utils

import prometheus_client as pm

pm_deleted_directories = pm.Counter(
    'biggraphite_cassandra_deleted_directories',
    'Number of directory that have been deleted so far')
pm_repaired_directories = pm.Counter(
    'biggraphite_cassandra_repaired_directories',
    'Number of missing directory created')
pm_expired_metrics = pm.Counter(
    'biggraphite_cassandra_expired_metrics',
    'Number of metrics that has been cleaned due to expiration')

log = logging.getLogger(__name__)

MINUTE = 60
HOUR = 60 * MINUTE
DAY = 24 * HOUR
GLOBSTAR = bg_glob.Globstar()
ANYSEQUENCE = bg_glob.AnySequence()

# Round the row size to 1000 seconds
_ROW_SIZE_PRECISION_MS = 1000 * 1000

DEFAULT_KEYSPACE = "biggraphite"
DEFAULT_CONTACT_POINTS = ["127.0.0.1"]
DEFAULT_PORT = 9042
DEFAULT_TIMEOUT = 10.0
# Disable compression per default as this is clearly useless for writes and
# reads do not generate that much traffic.
DEFAULT_COMPRESSION = False
# Current value is based on Cassandra page settings, so that everything fits in
# a single reply with default settings.
# TODO: Mesure actual number of metrics for existing queries and estimate a more
# reasonable limit, also consider other engines.
DEFAULT_MAX_METRICS_PER_PATTERN = 5000
DEFAULT_TRACE = False
DEFAULT_BULKIMPORT = False
DEFAULT_MAX_QUERIES_PER_PATTERN = 42
DEFAULT_MAX_CONCURRENT_QUERIES_PER_PATTERN = 4
DEFAULT_MAX_CONCURRENT_CONNECTIONS = 100
DEFAULT_MAX_BATCH_UTIL = 1000
DEFAULT_TIMEOUT_QUERY_UTIL = 120
DEFAULT_UPDATED_ON_TTL_SEC = 3 * DAY
DEFAULT_READ_ON_SAMPLING_RATE = 0.1

DIRECTORY_SEPARATOR = '.'

consistency_name_to_value = cassandra.ConsistencyLevel.name_to_value


def _consistency_validator(k):
    if k in consistency_name_to_value.keys():
        return k
    else:
        return None


OPTIONS = {
    "keyspace": str,
    "contact_points": _utils.list_from_str,
    "contact_points_metadata": _utils.list_from_str,
    "port": int,
    "port_metadata": lambda k: 0 if k is None else int(k),
    "timeout": float,
    "compression": _utils.bool_from_str,
    "max_metrics_per_pattern": int,
    "max_queries_per_pattern": int,
    "max_concurrent_queries_per_pattern": int,
    "max_concurrent_connections": int,
    "trace": bool,
    "bulkimport": bool,
    "enable_metrics": bool,
    "writer": lambda k: None if k is None else int(k),
    "replica": lambda k: 0 if k is None else int(k),
    "meta_write_consistency": _consistency_validator,
    "meta_serial_consistency": _consistency_validator,
    "meta_read_consistency": _consistency_validator,
    "meta_background_consistency": _consistency_validator,
    "data_write_consistency": _consistency_validator,
    "data_read_consistency": _consistency_validator,
    "updated_on_ttl_sec": int,
    "read_on_sampling_rate": float,
}


def add_argparse_arguments(parser):
    """Add Cassandra arguments to an argparse parser."""
    parser.add_argument(
        "--cassandra_keyspace", metavar="NAME",
        help="Cassandra keyspace.",
        default=DEFAULT_KEYSPACE)
    parser.add_argument(
        "--cassandra_contact_points", metavar="HOST[,HOST,...]",
        help="Hosts used for discovery.",
        default=DEFAULT_CONTACT_POINTS)
    parser.add_argument(
        "--cassandra_concurrent_connections", metavar="N", type=int,
        help="Maximum concurrent connections to the cluster.",
        default=DEFAULT_MAX_CONCURRENT_CONNECTIONS)
    parser.add_argument(
        "--cassandra_contact_points_metadata", metavar="HOST[,HOST,...]",
        help="Hosts used for discovery.",
        default=None)
    parser.add_argument(
        "--cassandra_port", metavar="PORT", type=int,
        help="The native port to connect to.",
        default=DEFAULT_PORT)
    parser.add_argument(
        "--cassandra_port_metadata", metavar="PORT", type=int,
        help="The native port to connect to.",
        default=None)
    parser.add_argument(
        "--cassandra_timeout", metavar="TIMEOUT", type=int,
        help="Cassandra query timeout in seconds.",
        default=DEFAULT_TIMEOUT)
    parser.add_argument(
        "--cassandra_compression", metavar="COMPRESSION", type=str,
        help="Cassandra network compression.",
        default=DEFAULT_COMPRESSION)
    parser.add_argument(
        "--cassandra_max_metrics_per_pattern",
        help="Maximum number of metrics returned for a glob query.",
        default=DEFAULT_MAX_METRICS_PER_PATTERN)
    parser.add_argument(
        "--cassandra_max_queries_per_pattern", type=int,
        help="Maximum number of sub-queries for any pattern-based query.",
        default=DEFAULT_MAX_QUERIES_PER_PATTERN)
    parser.add_argument(
        "--cassandra_max_concurrent_queries_per_pattern", type=int,
        help="Maximum number of concurrently executed sub-queries for any pattern-based query.",
        default=DEFAULT_MAX_CONCURRENT_QUERIES_PER_PATTERN)
    parser.add_argument(
        "--cassandra_trace",
        help="Enable query traces",
        default=DEFAULT_TRACE,
        action="store_true")
    parser.add_argument(
        "--cassandra_bulkimport", action="store_true",
        help="Generate files needed for bulkimport.")
    parser.add_argument(
        "--cassandra_enable_metrics",
        help="should expose metrics",
        action='store_true',
        default=False)
    parser.add_argument(
        "--cassandra_writer", type=int,
        help="Cassandra writer",
        default=None)
    parser.add_argument(
        "--cassandra_replica", type=int,
        help="Cassandra replica",
        default=None)
    parser.add_argument(
        "--meta_write_consistency", metavar="META_WRITE_CONS",
        help="Metadata write consistency",
        default=DEFAULT_META_WRITE_CONSISTENCY)
    parser.add_argument(
        "--meta_read_consistency", metavar="META_READ_CONS",
        help="Metadata read consistency",
        default=DEFAULT_META_READ_CONSISTENCY)
    parser.add_argument(
        "--meta_serial_consistency", metavar="META_SERIAL_CONS",
        help="Metadata serial consistency",
        default=DEFAULT_META_SERIAL_CONSISTENCY)
    parser.add_argument(
        "--meta_background_consistency", metavar="META_BACKGROUND_CONS",
        help="Metadata background consistency",
        default=DEFAULT_META_BACKGROUND_CONSISTENCY)
    parser.add_argument(
        "--data_write_consistency", metavar="DATA_WRITE_CONS",
        help="Data write consistency",
        default=DEFAULT_DATA_WRITE_CONSISTENCY)
    parser.add_argument(
        "--data_read_consistency", metavar="DATA_READ_CONS",
        help="Data read consistency",
        default=DEFAULT_DATA_READ_CONSISTENCY)
    parser.add_argument(
        "--cassandra_updated_on_ttl_sec",
        help="Update 'updated_on' field every x seconds.",
        default=DEFAULT_UPDATED_ON_TTL_SEC)
    parser.add_argument(
        "--cassandra_read_on_sampling_rate",
        help="Updated 'read_on' field every x calls.",
        default=DEFAULT_READ_ON_SAMPLING_RATE)


class Error(bg_accessor.Error):
    """Base class for all exceptions from this module."""


class CassandraError(Error):
    """Fatal errors accessing Cassandra."""


class RetryableCassandraError(CassandraError, bg_accessor.RetryableError):
    """Errors accessing Cassandra that could succeed if retried."""


class NotConnectedError(CassandraError):
    """Fatal errors accessing Cassandra because the Accessor is not connected."""


class TooManyMetrics(CassandraError):
    """A name glob yielded more than MAX_METRIC_PER_PATTERN metrics."""


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
DEFAULT_META_WRITE_CONSISTENCY = "ONE"
DEFAULT_META_SERIAL_CONSISTENCY = "LOCAL_SERIAL"
DEFAULT_META_READ_CONSISTENCY = "ONE"

DEFAULT_DATA_WRITE_CONSISTENCY = "ONE"
DEFAULT_DATA_READ_CONSISTENCY = "ONE"
DEFAULT_META_BACKGROUND_CONSISTENCY = "LOCAL_QUORUM"


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

_COMPONENTS_MAX_LEN = int(os.environ.get('BG_COMPONENTS_MAX_LEN', 64))
_LAST_COMPONENT = "__END__"
_METADATA_CREATION_CQL_PATH_COMPONENTS = ", ".join(
    "component_%d text" % n for n in range(_COMPONENTS_MAX_LEN)
)

_METADATA_CREATION_CQL_METRICS_METADATA = str(
    "CREATE TABLE IF NOT EXISTS \"%(keyspace)s\".metrics_metadata ("
    "  name text,"
    "  updated_on  timeuuid,"
    "  read_on  timeuuid,"
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

_METADATA_CREATION_CQL_METRICS_METADATA_READ_ON_INDEX = [
    "CREATE CUSTOM INDEX IF NOT EXISTS ON \"%%(keyspace)s\".%(table)s (read_on)"
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
                          + _METADATA_CREATION_CQL_METRICS_METADATA_READ_ON_INDEX
)

_DATAPOINTS_CREATION_CQL_TEMPLATE = str(
    "CREATE TABLE IF NOT EXISTS %(table)s ("
    "  metric uuid,"           # Metric UUID.
    "  time_start_ms bigint,"  # Lower bound for this row.
    "  offset smallint,"       # time_start_ms + offset * precision = timestamp
    "  shard  smallint,"       # Writer shard to allow restarts.
    "  value double,"          # Value for the point.
    "  count smallint,"        # If value is sum, divide by count to get the avg.
    "  PRIMARY KEY ((metric, time_start_ms), offset, shard)"
    ")"
    "  WITH CLUSTERING ORDER BY (offset DESC)"
    "  AND default_time_to_live = %(default_time_to_live)d"
    "  AND memtable_flush_period_in_ms = %(memtable_flush_period_in_ms)d"
    "  AND comment = '%(comment)s'"
    "  AND gc_grace_seconds = 86400"  # We don't do explicit deletes.
    "  AND compaction = {"
    "    'class': '%(compaction_strategy)s',"
    "    'timestamp_resolution': 'MICROSECONDS',"
    "    %(compaction_options)s"
    "  };"
)

# Special schema for first stages.
_DATAPOINTS0_CREATION_CQL_TEMPLATE = str(
    "CREATE TABLE IF NOT EXISTS %(table)s ("
    "  metric uuid,"           # Metric UUID.
    "  time_start_ms bigint,"  # Lower bound for this row.
    "  offset smallint,"       # time_start_ms + offset * precision = timestamp
    "  value double,"          # Value for the point.
    "  PRIMARY KEY ((metric, time_start_ms), offset)"
    ")"
    "  WITH CLUSTERING ORDER BY (offset DESC)"
    "  AND default_time_to_live = %(default_time_to_live)d"
    "  AND memtable_flush_period_in_ms = %(memtable_flush_period_in_ms)d"
    "  AND comment = '%(comment)s'"
    "  AND gc_grace_seconds = 86400"
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


REACTOR_TO_USE = None


def getConnectionClass():
    """Get connection class for cassandra."""
    global REACTOR_TO_USE

    if REACTOR_TO_USE in getConnectionClass.classes:
        return getConnectionClass.classes[REACTOR_TO_USE]

    if REACTOR_TO_USE == "TWISTED":
        from cassandra.io import twistedreactor as c_reactor
        CONNECTION_CLASS = c_reactor.TwistedConnection

    elif REACTOR_TO_USE == "LIBEV":
        from cassandra.io import libevreactor as c_libevreactor
        CONNECTION_CLASS = c_libevreactor.LibevConnection

    elif REACTOR_TO_USE == "ASYNC":
            from cassandra.io import asyncorereactor as c_asyncorereactor
            CONNECTION_CLASS = c_asyncorereactor.AsyncoreConnection
    else:
        try:
            from cassandra.io import libevreactor as c_libevreactor
            CONNECTION_CLASS = c_libevreactor.LibevConnection
        except ImportError:
            from cassandra.io import asyncorereactor as c_asyncorereactor
            CONNECTION_CLASS = c_asyncorereactor.AsyncoreConnection

    class _CappedConnection(CONNECTION_CLASS):
        """A connection with a cap on the number of in-flight requests per host."""

        # 300 is the minimum with protocol version 3, default is 65536
        max_in_flight = 600

    getConnectionClass.classes[REACTOR_TO_USE] = _CappedConnection
    return _CappedConnection


# We create a static variable to make sure that we don't create multiple
# classes if this function is called multiple time. The cassandra driver
# expect that only one class per event loop will be used.
getConnectionClass.classes = {}


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

    def __init__(self,
                 session,
                 keyspace,
                 shard,
                 bulkimport=False,
                 data_write_consistency=DEFAULT_DATA_WRITE_CONSISTENCY,
                 data_read_consistency=DEFAULT_DATA_READ_CONSISTENCY):
        self._keyspace = keyspace
        self._session = session
        self._bulkimport = bulkimport
        self.__stage_to_insert = {}
        self.__stage_to_select = {}
        self.__data_files = {}
        self._shard = shard
        self._data_write_consistency = consistency_name_to_value[data_write_consistency]
        self._data_read_consistency = consistency_name_to_value[data_read_consistency]

        release_version = session.get_pools()[0].host.release_version
        if version.LooseVersion(release_version) >= version.LooseVersion('3.9'):
            self._COMPACTION_STRATEGY = 'TimeWindowCompactionStrategy'
        else:
            self._COMPACTION_STRATEGY = _COMPACTION_STRATEGY

    def __bulkimport_filename(self, filename):
        current = multiprocessing.current_process()
        uid = str(current._identity[0]) if len(current._identity) else "0"
        filename = os_path.join("data", uid, filename)
        dirname = os_path.dirname(filename)
        if not os_path.exists(dirname):
            os.makedirs(dirname)
        return filename

    def _bulkimport_write_schema(self, stage, statement_str):
        filename = self.__bulkimport_filename(stage.as_full_string + ".cql")
        log.info("Writing schema for '%s' in '%s'" % (stage.as_full_string, filename))
        fh = open(filename, "w")
        fh.write(statement_str)
        fh.flush()
        fh.close()

    def _bulkimport_write_datapoint(self, stage, args):
        stage_str = stage.as_full_string
        if stage_str not in self.__data_files:
            statement_str = self._create_datapoints_table_stmt(stage)
            self._bulkimport_write_schema(stage, statement_str)

            filename = self.__bulkimport_filename(stage_str + ".csv")
            log.info("Writing data for '%s' in '%s'" % (stage_str, filename))
            fp = open(filename, "w", 1)
            self.__data_files[stage_str] = fp
        else:
            fp = self.__data_files[stage_str]
            fp.write(",".join([str(a) for a in args]) + "\n")

    def flush(self):
        for fp in self.__data_files.values():
            fp.flush()

    def _create_datapoints_table_stmt(self, stage):
        # Time after which data expire.
        time_to_live = stage.duration + _OUT_OF_ORDER_S

        # Estimate the age of the oldest data we still expect to read.
        fresh_time = stage.precision * _EXPECTED_POINTS_PER_READ

        cs_template = _DATAPOINTS_CREATION_CQL_CS_TEMPLATE.get(self._COMPACTION_STRATEGY)
        if not cs_template:
            raise InvalidArgumentError(
                "Unknown compaction strategy '%s'" % self._COMPACTION_STRATEGY)
        cs_kwargs = {}

        if self._COMPACTION_STRATEGY == "DateTieredCompactionStrategy":
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
        elif self._COMPACTION_STRATEGY == "TimeWindowCompactionStrategy":
            # TODO(c.chary): Tweak this once we have an actual 3.9 setup.

            window_size = min(
                # Documentation says that we should no more than 50 buckets.
                time_to_live / 50,
                max(
                    # But we don't want multiple sstables per hour.
                    HOUR,
                    # Also try to optimize for reads
                    fresh_time
                )
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
        comment = {
            "created_by": "biggraphite",
            "schema_version": 0,
            "stage": stage.as_full_string,
            "row_size_ms": _row_size_ms(stage),
            "row_size": _row_size_ms(stage) / stage.precision_ms,
        }
        kwargs = {
            "table": self._get_table_name(stage),
            "default_time_to_live": time_to_live,
            "memtable_flush_period_in_ms": _FLUSH_MEMORY_EVERY_S * 1000,
            "comment": json.dumps(comment),
            "compaction_strategy": self._COMPACTION_STRATEGY,
            "compaction_options": compaction_options,
        }

        if stage.stage0:
            template = _DATAPOINTS0_CREATION_CQL_TEMPLATE
        else:
            template = _DATAPOINTS_CREATION_CQL_TEMPLATE

        return template % kwargs

    def _create_datapoints_table(self, stage):
        # The statement is idempotent
        statement_str = self._create_datapoints_table_stmt(stage)
        self._session.execute(statement_str)

    def _get_table_name(self, stage):
        if stage.stage0:
            suffix = "_0"
        else:
            suffix = "_aggr"
        return "\"{}\".\"datapoints_{}p_{}s{}\"".format(
            self._keyspace, stage.points, stage.precision, suffix)

    def prepare_insert(self, stage, metric_id, time_start_ms, offset, value, count):
        statement = self.__stage_to_insert.get(stage)
        if stage.aggregated():
            args = (metric_id, time_start_ms, offset, self._shard, value, count)
        else:
            args = (metric_id, time_start_ms, offset, value)

        if self._bulkimport:
            self._bulkimport_write_datapoint(stage, args)
            return None, args

        if statement:
            return statement, args

        self._create_datapoints_table(stage)
        if stage.aggregated():
            statement_str = (
                "INSERT INTO %(table)s"
                " (metric, time_start_ms, offset, shard, value, count)"
                " VALUES (?, ?, ?, ?, ?, ?);"
            )
        else:
            statement_str = (
                "INSERT INTO %(table)s"
                " (metric, time_start_ms, offset, value)"
                " VALUES (?, ?, ?, ?);"
            )

        statement_str = statement_str % {"table": self._get_table_name(stage)}
        statement = self._session.prepare(statement_str)
        statement.consistency_level = self._data_write_consistency
        self.__stage_to_insert[stage] = statement
        return statement, args

    def prepare_select(self, stage, metric_id, row_start_ms, row_min_offset, row_max_offset):
        limit = (row_max_offset - row_min_offset) * bg_accessor.SHARD_MAX_REPLICAS
        args = (metric_id, row_start_ms, row_min_offset, row_max_offset, limit)

        # Don't execute useless queries.
        if limit <= 0:
            return None

        statement = self.__stage_to_select.get(stage)
        if statement:
            return statement, args

        self._create_datapoints_table(stage)
        if stage.aggregated():
            columns = ["time_start_ms", "offset", "shard", "value", "count"]
        else:
            columns = ["time_start_ms", "offset", "value"]

        statement_str = (
            "SELECT %(columns)s FROM %(table)s"
            " WHERE metric=? AND time_start_ms=?"
            " AND offset >= ? AND offset < ? "
            " ORDER BY offset"
            " LIMIT ?;"
        ) % {"columns": ", ".join(columns),
             "table": self._get_table_name(stage)}
        statement = self._session.prepare(statement_str)
        statement.consistency_level = self._data_read_consistency
        self.__stage_to_select[stage] = statement
        return statement, args


def expose_metrics(metrics, cluster_name=''):
    """Adaptor to notify prometheus of Cassandra's metrics change."""
    metrics_adp = {}

    def counter_adaptor(cpt, fn):
        def inner(*args, **kwargs):
            cpt.inc()
            fn(*args, **kwargs)

        return inner

    for attr in dir(metrics):
        if attr.startswith('on_'):
            metric_name = 'biggraphite_cassandra_' + cluster_name + '_' + attr[3:]
            cpt = pm.Counter(metric_name, '')
            metrics_adp[metric_name] = cpt
            setattr(metrics, attr, counter_adaptor(cpt, attr))

    return metrics_adp


class _CassandraAccessor(bg_accessor.Accessor):
    """Provides Read/Write accessors to Cassandra.

    Please refer to bg_accessor.Accessor.
    """

    _UUID_NAMESPACE = uuid.UUID('{00000000-1111-2222-3333-444444444444}')

    def __init__(self,
                 keyspace='biggraphite',
                 contact_points=DEFAULT_CONTACT_POINTS,
                 port=DEFAULT_PORT,
                 contact_points_metadata=None,
                 port_metadata=None,
                 timeout=DEFAULT_TIMEOUT,
                 compression=DEFAULT_COMPRESSION,
                 max_metrics_per_pattern=DEFAULT_MAX_METRICS_PER_PATTERN,
                 max_queries_per_pattern=DEFAULT_MAX_QUERIES_PER_PATTERN,
                 max_concurrent_queries_per_pattern=DEFAULT_MAX_CONCURRENT_QUERIES_PER_PATTERN,
                 trace=DEFAULT_TRACE,
                 max_concurrent_connections=DEFAULT_MAX_CONCURRENT_CONNECTIONS,
                 enable_metrics=False,
                 bulkimport=DEFAULT_BULKIMPORT,
                 writer=None,
                 replica=0,
                 meta_write_consistency=DEFAULT_META_WRITE_CONSISTENCY,
                 meta_read_consistency=DEFAULT_META_READ_CONSISTENCY,
                 meta_serial_consistency=DEFAULT_META_SERIAL_CONSISTENCY,
                 meta_background_consistency=DEFAULT_META_BACKGROUND_CONSISTENCY,
                 data_write_consistency=DEFAULT_DATA_WRITE_CONSISTENCY,
                 data_read_consistency=DEFAULT_DATA_READ_CONSISTENCY,
                 updated_on_ttl_sec=DEFAULT_UPDATED_ON_TTL_SEC,
                 read_on_sampling_rate=DEFAULT_READ_ON_SAMPLING_RATE):
        """Record parameters needed to connect.

        Args:
          keyspace: Base names of Cassandra keyspaces dedicated to BigGraphite.
          contact_points: list of strings, the hostnames or IP to use to discover Cassandra.
          port: The port to connect to, as an int.
          contact_points_metadata: list of strings, the hostnames or IP to use
            to discover Cassandra.
          port_metadata: The port to connect to, as an int.
          timeout: Default timeout for operations in seconds.
          compression: One of False, True, "lz4", "snappy"
          max_metrics_per_pattern: int, Maximum number of metrics per pattern.
          max_queries_per_pattern: int, Maximum number of sub-queries per pattern.
          max_concurrent_queries_per_pattern: int, Maximum number of
                                              concurrently executed sub-queries per pattern.
          trace: bool, Enabling query tracing.
          bulkimport: bool, Configure the accessor to generate files necessary for
            bulk import.
          writer: short, Id of the writer, this is used to handle different writers
            for the same metric and restarts.
          replica: shord, Id of the replica. Values will be grouped by replicas
            during read to allow multiple simultanous writers.
        """
        backend_name = "cassandra:" + keyspace
        super(_CassandraAccessor, self).__init__(backend_name)

        if not contact_points_metadata:
            contact_points_metadata = contact_points
        if not port_metadata:
            port_metadata = port

        self.keyspace = keyspace
        self.keyspace_metadata = keyspace + "_metadata"
        self.contact_points_data = contact_points
        self.contact_points_metadata = contact_points_metadata
        self.port = port
        self.port_metadata = port_metadata
        self.max_metrics_per_pattern = max_metrics_per_pattern
        self.max_queries_per_pattern = max_queries_per_pattern
        self.max_concurrent_queries_per_pattern = max_concurrent_queries_per_pattern
        self.max_concurrent_connections = max_concurrent_connections
        self.__compression = compression
        self.__trace = trace
        self.__bulkimport = bulkimport
        self.__metadata_touch_ttl_sec = updated_on_ttl_sec,
        self.__downsampler = _downsampling.Downsampler()
        self.__delayed_writer = _delayed_writer.DelayedWriter(self)
        self.__cluster_data = None  # setup by connect()
        self.__cluster_metadata = None  # setup by connect()
        self.__lazy_statements = None  # setup by connect()
        self.__timeout = timeout
        self.__insert_metric_statement = None  # setup by connect()
        self.__select_metric_metadata_statement = None  # setup by connect()
        self.__update_metric_metadata_statement = None  # setup by connect()
        self.__touch_metrics_metadata_statement = None  # setup by connect()
        self.__session_data = None  # setup by connect()
        self.__session_metadata = None  # setup by connect()
        self.__glob_parser = bg_glob.GraphiteGlobParser()
        self.__metrics = {}
        self.__enable_metrics = enable_metrics
        self.__read_on_counter = 0
        self.__read_on_sampling_rate = read_on_sampling_rate
        self._meta_write_consistency = consistency_name_to_value[meta_write_consistency]
        self._meta_read_consistency = consistency_name_to_value[meta_read_consistency]
        self._meta_serial_consistency = consistency_name_to_value[meta_serial_consistency]
        self._meta_background_consistency = consistency_name_to_value[meta_background_consistency]
        self._data_write_consistency = consistency_name_to_value[data_write_consistency]
        self._data_read_consistency = consistency_name_to_value[data_read_consistency]
        if writer is None:
            # TODO: Currently a random shard is good enough.
            # We should use a counter stored in cassandra instead.
            writer = bg_accessor.pack_shard(
                replica, random.getrandbits(bg_accessor.SHARD_WRITER_BITS))
        # Cassandra expects a signed short, make sure we give it something
        # it understands.
        self.__shard = self.__shard = bg_accessor.pack_shard(replica, writer)
        self.__shard = c_marshal.int16_unpack(c_marshal.uint16_pack(self.__shard))

    def connect(self):
        """See bg_accessor.Accessor."""
        super(_CassandraAccessor, self).connect()
        self._connect_clusters()
        self._prepare_statements()
        self.is_connected = True

    @property
    def shard(self):
        return self.__shard

    @shard.setter
    def shard(self, value):
        self.__shard = value
        if self.__lazy_statements:
            self.__lazy_statements._shard = value

    def _prepare_statements(self):
        def __prepare(cql, consistency=self._meta_write_consistency):
            statement = self.__session_metadata.prepare(cql)
            statement.consistency_level = consistency
            return statement

        # Metadata (metrics and directories)
        components_names = ", ".join("component_%d" % n for n in range(_COMPONENTS_MAX_LEN))
        components_marks = ", ".join("?" for n in range(_COMPONENTS_MAX_LEN))
        self.__insert_metric_statement = __prepare(
            "INSERT INTO \"%s\".metrics (name, parent, %s) VALUES (?, ?, %s);"
            % (self.keyspace_metadata, components_names, components_marks)
        )
        self.__insert_directory_statement = __prepare(
            "INSERT INTO \"%s\".directories (name, parent, %s) VALUES (?, ?, %s) IF NOT EXISTS;"
            % (self.keyspace_metadata, components_names, components_marks)
        )
        self.__insert_directory_statement.serial_consistency_level = self._meta_serial_consistency
        # We do not set the serial_consistency, it defautls to SERIAL.
        self.__select_metric_metadata_statement = __prepare(
            "SELECT id, config, toUnixTimestamp(updated_on)"
            " FROM \"%s\".metrics_metadata WHERE name = ?;"
            % self.keyspace_metadata,
            self._meta_read_consistency
        )
        self.__select_metric_statement = __prepare(
            "SELECT * FROM \"%s\".metrics WHERE name = ?;"
            % self.keyspace_metadata,
            self._meta_read_consistency
        )
        self.__select_directory_statement = __prepare(
            "SELECT * FROM \"%s\".directories WHERE name = ?;"
            % self.keyspace_metadata,
            self._meta_read_consistency
        )
        self.__update_metric_metadata_statement = __prepare(
            "UPDATE \"%s\".metrics_metadata SET config=?, updated_on=now()"
            " WHERE name=?;" % self.keyspace_metadata
        )
        self.__touch_metrics_metadata_statement = __prepare(
            "UPDATE \"%s\".metrics_metadata SET updated_on=now()"
            " WHERE name=?;" % self.keyspace_metadata
        )
        self.__insert_metrics_metadata_statement = __prepare(
            "INSERT INTO \"%s\".metrics_metadata (name, updated_on, id, config)"
            " VALUES (?, now(), ?, ?);" % self.keyspace_metadata
        )
        self.__update_metric_read_on_metadata_statement = __prepare(
            "UPDATE \"%s\".metrics_metadata SET read_on=now()"
            " WHERE name=?;" % self.keyspace_metadata
        )

    def _connect_clusters(self):
        self.__cluster_metadata, self.__session_metadata = self._connect(
            self.contact_points_metadata, self.port_metadata)
        if self.contact_points_data != self.contact_points_metadata:
            self.__cluster_data, self.__session_data = self._connect(
                self.contact_points_data, self.port)
        else:
            self.__session_data = self.__session_metadata
            self.__cluster_data = self.__cluster_metadata

        self.__lazy_statements = _LazyPreparedStatements(
            self.__session_data, self.keyspace, self.__shard, self.__bulkimport)

        if self.__enable_metrics:
            self.__metrics["metadata"] = expose_metrics(self.__cluster_metadata.metrics, 'metadata')
            self.__metrics["data"] = expose_metrics(self.__cluster_data.metrics, 'data')

    def _connect(self, contact_points, port):
        cluster = c_cluster.Cluster(
            contact_points, port,
            compression=self.__compression,
            metrics_enabled=False,
        )

        # Limits in flight requests
        cluster.connection_class = getConnectionClass()
        session = cluster.connect()
        session.row_factory = c_query.tuple_factory  # Saves 2% CPU
        if self.__timeout:
            session.default_timeout = self.__timeout
        return cluster, session

    def _execute(self, session, *args, **kwargs):
        """Wrapper for __session.execute_async()."""
        if self.__bulkimport:
            return []

        if self.__trace:
            kwargs["trace"] = True

        if args:
            log.debug(' '.join([str(arg) for arg in args]))
        if kwargs:
            log.debug(' '.join(["%s:%s " % (k, v) for k, v in kwargs.items()]))
        result = session.execute(*args, **kwargs)

        if self.__trace:
            trace = result.get_query_trace()
            for e in trace.events:
                log.debug("%s: %s" % (e.source_elapsed, str(e)))
        return result

    def _execute_data(self, *args, **kwargs):
        return self._execute(self.__session_data, *args, **kwargs)

    def _execute_metadata(self, *args, **kwargs):
        return self._execute(self.__session_metadata, *args, **kwargs)

    def _execute_async(self, session, *args, **kwargs):
        """Wrapper for __session.execute_async()."""
        if self.__bulkimport:
            class _FakeFuture(object):
                def add_callbacks(self, on_result, on_failure):
                    on_result(None)
            return _FakeFuture()

        if self.__trace:
            kwargs["trace"] = True

        if args:
            log.debug(' '.join([str(arg) for arg in args]))
        if kwargs:
            log.debug(' '.join(["%s:%s " % (k, v) for k, v in kwargs.items()]))
        future = session.execute_async(*args, **kwargs)

        if self.__trace:
            trace = future.get_query_trace()
            for e in trace.events:
                log.debug(e.source_elapsed, e.description)
        return future

    def _execute_async_data(self, *args, **kwargs):
        return self._execute_async(self.__session_data, *args, **kwargs)

    def _execute_async_metadata(self, *args, **kwargs):
        return self._execute_async(self.__session_metadata, *args, **kwargs)

    def _execute_concurrent(self, session, statements_and_parameters, **kwargs):
        """Wrapper for concurrent.execute_concurrent()."""
        if self.__bulkimport:
            return []

        log.debug(statements_and_parameters)

        if not self.__trace:
            return c_concurrent.execute_concurrent(
                session, statements_and_parameters, **kwargs)

        query_results = []
        for statement, params in statements_and_parameters:
            try:
                result = self._execute(session, statement, params, trace=True)
                success = True
            except Exception as e:
                if kwargs.get('raise_on_first_error') is True:
                    raise e
                result = e
                success = False
                query_results.append((success, result))
        return query_results

    def _execute_concurrent_data(self, *args, **kwargs):
        return self._execute_concurrent(self.__session_data, *args, **kwargs)

    def _execute_concurrent_metadata(self, *args, **kwargs):
        return self._execute_concurrent(self.__session_metadata, *args, **kwargs)

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
        if parent_dir and not self.has_directory(parent_dir):
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

        self._execute_concurrent_metadata(
            queries,
            raise_on_first_error=False)

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
        self._execute_metadata(
            self.__update_metric_metadata_statement,
            [metadata_dict, encoded_metric_name])

    def _create_parent_dirs_queries(self, components):
        queries = []
        path = []
        parent = ''
        # Incrementally construct parents, till we reach the leaf component.
        # -2 skips leaf (metric name) and _LAST_COMPONENT marker.
        for component in components[:-2]:
            path.append(component)
            name = DIRECTORY_SEPARATOR.join(path)
            path_components = path + [_LAST_COMPONENT]
            padding_len = _COMPONENTS_MAX_LEN - len(path_components)
            padding = [c_query.UNSET_VALUE] * padding_len
            queries.append((
                self.__insert_directory_statement,
                [name, parent + DIRECTORY_SEPARATOR] + path_components + padding,
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
            session = self.__session_data if self.keyspace else self.__session_metadata
            statement_str = "SELECT table_name FROM system_schema.tables WHERE keyspace_name = %s;"
            tables = [r[0] for r in self._execute(
                session, statement_str, (keyspace, ))]

            for table in tables:
                self._execute(
                    session, "TRUNCATE \"%s\".\"%s\";" % (keyspace, table))

    def fetch_points(self, metric, time_start, time_end, stage, aggregated=True):
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
        query_results = self._execute_concurrent_data(
            statements_and_args,
            results_generator=True,
        )
        return bg_accessor.PointGrouper(
            metric, time_start_ms, time_end_ms, stage, query_results, aggregated=aggregated
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
            if select is not None:
                res.append(select)

        return res

    def _update_metric_read_on(self, metric_name):
        queries = []
        rate = int(1 / self.__read_on_sampling_rate)
        if self.__read_on_counter % rate == 0:
            log.debug('updating read_on for %s' % metric_name)
            queries.append((
                self.__update_metric_read_on_metadata_statement,
                [metric_name],
            ))
            self._execute_concurrent_metadata(
                queries,
                raise_on_first_error=False)

    def _select_metric(self, metric_name):
        """Fetch metric metadata."""
        self.__read_on_counter += 1
        encoded_metric_name = bg_accessor.encode_metric_name(metric_name)
        result = list(self._execute_metadata(
            self.__select_metric_metadata_statement, (encoded_metric_name, )))
        if not result:
            return None
        self._update_metric_read_on(metric_name)
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
        if parent_dir and not self.has_directory(parent_dir):
            return False

        return True

    def has_directory(self, directory):
        encoded_directory = bg_accessor.encode_metric_name(directory)
        result = list(self._execute_metadata(
            self.__select_directory_statement, (encoded_directory, )))
        return bool(result)

    def __touch_metadata_on_need(self, metric_name, updated_on):
        if (int(time.time()) - int(updated_on / 1000)) >= self.__metadata_touch_ttl_sec:
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

        # Return None if any of the important column is missing.
        if not result[0] or not result[1]:
            return None

        self.__touch_metadata_on_need(metric_name, updated_on)
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
        if glob == "":
            return []

        components = self.__glob_parser.parse(glob)
        if len(components) > _COMPONENTS_MAX_LEN:
            raise bg_accessor.InvalidGlobError(
                "Contains %d components, but we support %d at most" % (
                    len(components),
                    _COMPONENTS_MAX_LEN,
                )
            )

        if GLOBSTAR in components:
            queries = self.__generate_globstar_names_queries(table, components)
        else:
            components.append([_LAST_COMPONENT])
            queries = self.__generate_normal_names_queries(table, components)

        if self.cache:
            cached_results = self.cache.get_many(queries)
            for query in cached_results:
                queries.remove(query)
        else:
            cached_results = {}

        statements_with_params = []
        for query in queries:
            statement = c_query.SimpleStatement(
                query,
                consistency_level=self._meta_read_consistency,
            )
            statements_with_params.append((statement, ()))

        query_results = self._execute_concurrent_metadata(
            statements_with_params,
            concurrency=self.max_concurrent_queries_per_pattern,
            results_generator=True,
            raise_on_first_error=True,
        )

        def _extract_results(query_results):
            n_metrics = 0
            fetched_results = collections.defaultdict(list)

            too_many_metrics = TooManyMetrics(
                "Query %s on %s yields more than %d results" %
                (glob, table, self.max_metrics_per_pattern)
            )

            for query, names in cached_results.items():
                for name in names:
                    n_metrics += 1
                    if n_metrics > self.max_metrics_per_pattern:
                        raise too_many_metrics
                    yield name

            try:
                for success, results in query_results:
                    query = results.response_future.query.query_string
                    for result in results:
                        n_metrics += 1
                        name = result[0]
                        fetched_results[query].append(name)
                        if n_metrics > self.max_metrics_per_pattern:
                            raise too_many_metrics
                        yield name
            except cassandra.DriverException as e:
                raise CassandraError('Failed to glob: %s on %s' % (table, glob), e)

            if self.cache:
                self.cache.set_many(fetched_results, timeout=self.cache_metadata_ttl)

        return _extract_results(query_results)

    def __generate_normal_names_queries(self, table, components):
        # Only keep the component parts that enable us to build prefix queries.
        # This means any uninterrupted sequence of strings or braces selectors.
        # On the way, we keep the position and value counts of selectors for
        # further query simplification.
        idxlens = []
        combinations = 1
        for cidx, component in enumerate(components):
            entry = []
            end = 0
            for pidx, part in enumerate(component):
                if not isinstance(part, (unicode, str, bg_glob.SequenceIn)):
                    break
                elif isinstance(part, bg_glob.SequenceIn):
                    count = len(part.values)
                    combinations *= count
                    entry.append((pidx, count))

                end = pidx + 1

            idxlens.append(entry)
            simplified_component = component[:end]
            if len(simplified_component) < len(component):
                simplified_component.append(ANYSEQUENCE)

            components[cidx] = simplified_component

        # Skip any additional work if we have a basic query.
        if combinations == 1:
            return [self.__build_select_names_query(table, components)]

        # Reduce complexity by dropping the rightmost selector from each
        # component, starting with the shallowest component, until we have a low
        # enough combination count.
        for cidx, entry in enumerate(idxlens):
            if combinations <= self.max_queries_per_pattern:
                break

            while len(entry) > 0 and combinations > self.max_queries_per_pattern:
                component = components[cidx]
                idx, count = entry.pop()

                surrounding_anyseqs = 0
                if idx > 0 and component[idx-1] == ANYSEQUENCE:
                    surrounding_anyseqs += 1
                if idx < len(component) - 1 and component[idx+1] == ANYSEQUENCE:
                    surrounding_anyseqs += 1

                # If we have surrounding AnySeqs, then drop elements so that
                # only one remains. Otherwise, replace current part with AnySeq.
                if surrounding_anyseqs > 0:
                    while surrounding_anyseqs > 0:
                        del(component[idx])
                        surrounding_anyseqs -= 1
                else:
                    component[idx] = ANYSEQUENCE

                combinations /= count

        # Pre-compute all possible values for each component.
        for cidx, component in enumerate(components):
            suffix = []
            if component[-1] == ANYSEQUENCE:
                if len(component) == 1:
                    components[cidx] = [component]
                    continue
                else:
                    suffix.append(ANYSEQUENCE)

            values = ['']
            for part in component:
                if isinstance(part, (unicode, str)):
                    values = [x + part for x in values]
                elif isinstance(part, bg_glob.SequenceIn):
                    values = [x + y
                              for x in values
                              for y in part.values]
                else:
                    break

            components[cidx] = [[x] + suffix for x in values]

        # Generate queries using the combinations of pre-computed values for the
        # components.
        return [
            self.__build_select_names_query(table, combination)
            for combination in itertools.product(*components)
        ]

    def __generate_globstar_names_queries(self, table, components):
        # Handling more than one of these can cause combinatorial explosion.
        if components.count(GLOBSTAR) > 1:
            raise bg_accessor.InvalidGlobError(
                "Contains more than one globstar (**) operator"
            )

        # If the globstar operator is at the end of the pattern, then we can
        # find corresponding metrics with a prefix search;
        # otherwise, we have to generate incremental queries that go up to a
        # certain depth (_COMPONENTS_MAX_LEN - #components).
        gs_index = components.index(GLOBSTAR)
        if gs_index == len(components) - 1:
            return [
                self.__build_select_names_query(table, components[:gs_index])
            ]

        prefix = components[:gs_index]
        suffix = components[gs_index+1:] + [[_LAST_COMPONENT]]
        max_wildcards = min(self.max_queries_per_pattern,
                            _COMPONENTS_MAX_LEN - len(components))
        return [
            self.__build_select_names_query(
                table,
                prefix + wildcards * [[bg_glob.AnySequence()]] + suffix,
            )
            for wildcards in range(1, max_wildcards)
        ]

    def __build_select_names_query(self, table, components):
        query_select = "SELECT name FROM \"%s\".\"%s\"" % (
            self.keyspace_metadata,
            table,
        )
        query_limit = "LIMIT %d" % (self.max_metrics_per_pattern + 1)

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
            all(len(c) == 1 and isinstance(c[0], (unicode, str))
                for c in components[:-2])
        ):
            last = components[-2]
            if len(last) == 1 and isinstance(last[0], (unicode, str)):
                # XXX(d.forest): do not try to optimize by passing the raw glob
                #                and using it here; because this is invalid in
                #                cases where the glob contains braces.
                name = DIRECTORY_SEPARATOR.join(itertools.chain.from_iterable(components[:-1]))
                return "%s WHERE name = %s %s;" % (
                    query_select,
                    c_encoder.cql_quote(name),
                    query_limit,
                )
            else:
                if len(last) > 0 and isinstance(last[0], (unicode, str)):
                    prefix_filter = "AND component_%d LIKE %s" % (
                        len(components) - 2,
                        c_encoder.cql_quote(last[0] + '%'),
                    )
                    allow_filtering = "ALLOW FILTERING"
                else:
                    prefix_filter = ''
                    allow_filtering = ''

                parent = itertools.chain.from_iterable(components[:-2])
                parent = DIRECTORY_SEPARATOR.join(parent) + DIRECTORY_SEPARATOR
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
            if not isinstance(value, (unicode, str)):
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

    def background(self):
        """Perform periodic background operations."""
        if self.__downsampler:
            self.__downsampler.purge()
        if self.__delayed_writer:
            self.__delayed_writer.write_some()

    def flush(self):
        """Flush any internal buffers."""
        if self.__delayed_writer:
            self.__delayed_writer.flush()
        self.__lazy_statements.flush()

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

        statements = collections.defaultdict(list)
        for timestamp, value, count, stage in datapoints:
            timestamp_ms = int(timestamp) * 1000
            time_offset_ms = timestamp_ms % _row_size_ms(stage)
            time_start_ms = timestamp_ms - time_offset_ms
            offset = stage.step_ms(time_offset_ms)

            statement, args = self.__lazy_statements.prepare_insert(
                stage=stage, metric_id=metric.id, time_start_ms=time_start_ms,
                offset=offset, value=value, count=count,
            )

            # We group by table/partition.
            key = (stage, time_start_ms)
            statements[key].append((statement, args))

        for statements_and_args in statements.values():
            if len(statements_and_args) == 1:
                statement, args = statements_and_args[0]
                count = 1
            else:
                batch = c_query.BatchStatement(
                    consistency_level=self._data_write_consistency,
                    batch_type=c_query.BatchType.UNLOGGED
                )
                for statement, args in statements_and_args:
                    if statement is not None:
                        batch.add(statement, args)
                statement = batch
                args = None
                count = len(statements_and_args)

            future = self._execute_async(
                session=self.__session_data, query=statement, parameters=args)
            if count_down:
                if count > 1:
                    # If batched we will get less results.
                    count_down.count -= count - 1
                future.add_callbacks(
                    count_down.on_cassandra_result,
                    count_down.on_cassandra_failure,
                )

    def shutdown(self):
        """See bg_accessor.Accessor."""
        super(_CassandraAccessor, self).shutdown()
        try:
            if self.__cluster_data:
                self.__cluster_data.shutdown()
        except Exception as exc:
            raise CassandraError(exc)
        self.__cluster_data = None

        try:
            if self.__cluster_metadata:
                self.__cluster_metadata.shutdown()
        except Exception as exc:
            raise CassandraError(exc)
        self.__cluster_metadata = None

        self.is_connected = False

    def syncdb(self, dry_run=False):
        schema = ""

        self._connect_clusters()
        self.__cluster_data.refresh_schema_metadata()
        self.__cluster_metadata.refresh_schema_metadata()

        keyspaces = self.__cluster_metadata.metadata.keyspaces.keys()
        if self.keyspace_metadata not in keyspaces:
            raise CassandraError("Missing keyspace '%s'." % self.keyspace_metadata)

        keyspaces = self.__cluster_data.metadata.keyspaces.keys()
        if self.keyspace not in keyspaces:
            raise CassandraError("Missing keyspace '%s'." % self.keyspace)

        for cql in _METADATA_CREATION_CQL:
            query = cql % {"keyspace": self.keyspace_metadata}
            schema += query + "\n\n"
            if not dry_run:
                self._execute_metadata(query)

        tables = self.__cluster_data.metadata.keyspaces[self.keyspace].tables
        for table in tables:
            comment = tables[table].options.get('comment')
            if table.startswith('datapoints_'):
                if not comment:
                    continue
                stage_str = json.loads(comment).get('stage')
                if not stage_str:
                    continue
                try:
                    stage = bg_accessor.Stage.from_string(stage_str)
                except Exception as e:
                    log.debug(e)
                    continue
                query = self.__lazy_statements._create_datapoints_table_stmt(stage)
                schema += query + "\n\n"

        self.shutdown()
        return schema

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
            self._execute_metadata(statement, args)

    def map(self, callback, start_key=None, end_key=None, shard=1, nshards=0):
        """See bg_accessor.Accessor.

        Slight change for start_key and end_key, they are intrepreted as
        tokens directly.
        """

        start_token, stop_token = self._get_search_range(start_key, end_key, shard, nshards)

        select = self._prepare_background_request(
            "SELECT name, token(name), id, config FROM \"%s\".metrics_metadata"
            " WHERE token(name) > ? LIMIT %d ;"
            % (self.keyspace_metadata, DEFAULT_MAX_BATCH_UTIL))
        select.request_timeout = None

        token = start_token
        while token < stop_token:
            rows = self._execute_metadata(select, (token,), DEFAULT_TIMEOUT_QUERY_UTIL)
            if len(rows.current_rows) == 0:
                break

            token = rows[-1][1]

            done = token - start_token
            total = stop_token - start_token

            for i, result in enumerate(rows):
                metric_name = result[0]
                id = result[2]
                config = result[3]

                metadata = bg_accessor.MetricMetadata.from_string_dict(config)
                metric = bg_accessor.Metric(metric_name, id, metadata)
                callback(metric, done + 1, total)

    def repair(self, start_key=None, end_key=None, shard=0, nshards=1,
               callback_on_progress=None):
        """See bg_accessor.Accessor.

        Slight change for start_key and end_key, they are intrepreted as
        tokens directly.
        """
        super(_CassandraAccessor, self).repair(start_key, end_key, shard, nshards)
        self._repair_missing_dir(start_key, end_key, shard, nshards, callback_on_progress)

    def _get_search_range(self, start_key, end_key, shard, nshards):
        partitioner = self.__cluster_data.metadata.partitioner
        if partitioner != "org.apache.cassandra.dht.Murmur3Partitioner":
            raise "Partitioner '%s' not supported" % partitioner

        start_token = murmur3.INT64_MIN if not start_key else int(start_key)
        stop_token = murmur3.INT64_MAX if not end_key else int(end_key)

        if nshards > 1:
            tokens = stop_token - start_token
            my_tokens = tokens / nshards
            start_token += my_tokens * shard
            stop_token = start_token + my_tokens

        return (start_token, stop_token)

    def _repair_missing_dir(self, start_key=None, end_key=None, shard=0, nshards=1,
                            callback_on_progress=None):
        """Create directory that does not exist for a metric to be accessible."""
        start_token, stop_token = self._get_search_range(start_key, end_key, shard, nshards)

        dir_query = self._prepare_background_request(
            "SELECT name, token(name) FROM \"%s\".directories"
            " WHERE token(name) > ? LIMIT %d;"
            % (self.keyspace_metadata, DEFAULT_MAX_BATCH_UTIL))

        has_directory_query = self._prepare_background_request(
            "SELECT name FROM \"%s\".directories"
            " WHERE name = ? LIMIT 1;"
            % self.keyspace_metadata)

        def directories_to_check(result):
            for row in result:
                name, next_token = row
                parent_dir = name.rpartition(".")[0]
                if parent_dir:
                    yield has_directory_query, (parent_dir,)

        def directories_to_create(result):
            for response in result:
                if not response.success:
                    log.warning(str(response.result_or_exc))
                    continue
                results = list(response.result_or_exc)
                if results:
                    continue
                dir_name = response.result_or_exc.response_future.query.values[0]
                log.info("Scheduling repair for '%s'" % dir_name)
                components = self._components_from_name(dir_name + DIRECTORY_SEPARATOR + '_')
                queries = self._create_parent_dirs_queries(components)
                for query in queries:
                    pm_repaired_directories.inc()
                    yield query

        log.info("Start creating missing directories")
        token = start_token
        while token < stop_token:
            result = self._execute_metadata(dir_query, (token,), DEFAULT_TIMEOUT_QUERY_UTIL)
            if len(result.current_rows) == 0:
                break

            # Update token range for the next iteration
            token = result[-1][1]
            parent_dirs = self._execute_concurrent_metadata(
                directories_to_check(result),
                concurrency=self.max_concurrent_connections,
                raise_on_first_error=False)

            rets = self._execute_concurrent_metadata(
                directories_to_create(parent_dirs),
                concurrency=self.max_concurrent_connections,
                raise_on_first_error=False)

            for ret in rets:
                if not ret.success:
                    log.warn(str(ret.result_or_exc))

            if callback_on_progress:
                callback_on_progress(token - start_token, stop_token - start_token)

    def _clean_empty_dir(self, start_key=None, end_key=None, shard=0, nshards=1,
                         callback_on_progress=None):
        """Remove directory that does not contains any metrics."""
        start_token, stop_token = self._get_search_range(start_key, end_key, shard, nshards)

        dir_query = self._prepare_background_request(
            "SELECT name, token(name)"
            " FROM \"%s\".directories"
            " WHERE token(name) > ? LIMIT %d;"
            % (self.keyspace_metadata,
               DEFAULT_MAX_BATCH_UTIL))
        has_metric_query = self._prepare_background_request(
            "SELECT name FROM \"%s\".metrics"
            " WHERE parent LIKE ? LIMIT 1;"
            % (self.keyspace_metadata, ))
        delete_empty_dir_stm = self._prepare_background_request(
            "DELETE FROM \"%s\".directories"
            " WHERE name = ?;"
            % self.keyspace_metadata)

        def directories_to_check(result):
            for row in result:
                name, next_token = row
                if name:
                    yield (has_metric_query, (name + DIRECTORY_SEPARATOR + '%',))

        def directories_to_remove(result):
            for response in result:
                if not response.success:
                    log.warning(str(response.result_or_exc))
                    continue
                results = list(response.result_or_exc)
                if results:
                    continue
                dir_name = response.result_or_exc.response_future.query.values[0]
                dir_name = dir_name.rpartition('.')[0]
                log.info("Scheduling delete for '%s'" % dir_name)
                pm_deleted_directories.inc()
                yield delete_empty_dir_stm, (dir_name,)

        log.info("Starting cleanup of empty dir")
        token = start_token
        while token < stop_token:
            result = self._execute_metadata(dir_query, (token,), DEFAULT_TIMEOUT_QUERY_UTIL)
            if len(result.current_rows) == 0:
                break

            # Update token range for the next iteration
            token = result[-1][1]
            parent_dirs = self._execute_concurrent_metadata(
                directories_to_check(result),
                concurrency=self.
                max_concurrent_connections,
                raise_on_first_error=False)
            rets = self._execute_concurrent_metadata(
                directories_to_remove(parent_dirs),
                concurrency=self.max_concurrent_connections,
                raise_on_first_error=False)
            for ret in rets:
                if not ret.success:
                    log.warning(str(ret.result_or_exc))

            if callback_on_progress:
                callback_on_progress(token - start_token, stop_token - start_token)

    def clean(self, max_age=None, start_key=None, end_key=None, shard=1, nshards=0,
              callback_on_progress=None):
        """See bg_accessor.Accessor.

        Args:
            cutoff: UNIX time in seconds. Rows older than it should be deleted.
        """
        super(_CassandraAccessor, self).clean(max_age, callback_on_progress)

        self._clean_expired_metrics(max_age, start_key, end_key, shard, nshards,
                                    callback_on_progress)
        self._clean_empty_dir(start_key, end_key, shard, nshards, callback_on_progress)

    def _prepare_background_request(self, query_str):
        select = self.__session_metadata.prepare(query_str)
        select.consistency_level = self._meta_background_consistency
        select.retry_policy = cassandra.policies.DowngradingConsistencyRetryPolicy
        select.request_timeout = DEFAULT_TIMEOUT_QUERY_UTIL

        return select

    def _clean_expired_metrics(self, max_age=None, start_key=None, end_key=None, shard=1, nshards=0,
                               callback_on_progress=None):
        """Delete metrics that has an expired ttl."""
        if not max_age:
            log.warn("You must specify a cutoff time for cleanup")
            return

        start_token, stop_token = self._get_search_range(start_key, end_key, shard, nshards)

        # timestamp format in Cassandra is in milliseconds
        cutoff = (int(time.time()) - max_age) * 1000
        log.info("Cleaning with cutoff time %d", cutoff)

        # statements
        select = self._prepare_background_request(
            "SELECT name, token(name) FROM \"%s\".metrics_metadata"
            " WHERE updated_on <= maxTimeuuid(%d) and token(name) > ? LIMIT %d ;"
            % (self.keyspace_metadata, cutoff, DEFAULT_MAX_BATCH_UTIL))
        select.request_timeout = None

        delete = self._prepare_background_request(
            "DELETE FROM \"%s\".metrics WHERE name = ? ;" %
            (self.keyspace_metadata))
        delete_metadata = self._prepare_background_request(
            "DELETE FROM \"%s\".metrics_metadata WHERE name = ? ;" %
            (self.keyspace_metadata))

        def run(rows):
            for name, _ in rows:
                log.info("Scheduling delete for %s", name)
                pm_expired_metrics.inc()
                yield (delete, (name,))
                yield (delete_metadata, (name,))

        token = start_token
        while token < stop_token:
            rows = self._execute_metadata(select, (token,), DEFAULT_TIMEOUT_QUERY_UTIL)
            if len(rows.current_rows) == 0:
                break

            token = rows[-1][1]
            rets = self._execute_concurrent_metadata(
                run(rows),
                concurrency=self.max_concurrent_connections,
                raise_on_first_error=False)

            for ret in rets:
                if not ret.success:
                    log.warn(str(ret.result_or_exc))

            if callback_on_progress:
                callback_on_progress(token - start_token, stop_token - start_token)


def build(*args, **kwargs):
    """Return a bg_accessor.Accessor using Casssandra.

    Args:
      keyspace: Base name of Cassandra keyspaces dedicated to BigGraphite.
      contact_points: list of strings, the hostnames or IP to use to discover Cassandra.
      port: The port to connect to, as an int.
    """
    return _CassandraAccessor(*args, **kwargs)
