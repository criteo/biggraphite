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
import datetime
import json
import gzip
import logging
import multiprocessing
import os
import random
import time
from distutils import version
from os import path as os_path
from os import environ

import cassandra
import prometheus_client
import six
from cassandra import auth
from cassandra import cluster as c_cluster
from cassandra import concurrent as c_concurrent
from cassandra import marshal as c_marshal
from cassandra import murmur3
from cassandra import policies as c_policies
from cassandra import query as c_query
from future.utils import raise_with_traceback

from biggraphite import accessor as bg_accessor, tracing
from biggraphite import glob_utils as bg_glob
from biggraphite import metric as bg_metric
from biggraphite import utils as bg_utils
from biggraphite.drivers import _delayed_writer
from biggraphite.drivers import _downsampling
from biggraphite.drivers import _utils
from biggraphite.drivers import cassandra_common as common
from biggraphite.drivers import cassandra_policies as bg_cassandra_policies
from biggraphite.drivers import cassandra_sasi as sasi
from biggraphite.drivers import cassandra_stratio_lucene as lucene
from biggraphite.drivers.ttls import DAY, HOUR, MINUTE
from biggraphite.drivers.ttls import DEFAULT_UPDATED_ON_TTL_SEC

from ssl import SSLContext, PROTOCOL_TLS, CERT_REQUIRED, CERT_NONE

DELETE_METRIC_METADATA = prometheus_client.Counter(
    "bg_cassandra_delete_metric_metadata",
    "Number of metrics metadata that have been deleted",
)
DELETE_DIRECTORY = prometheus_client.Counter(
    "bg_cassandra_delete_directory",
    "Number of directories that have been deleted",
)
DELETE_METRIC = prometheus_client.Counter(
    "bg_cassandra_delete_metric",
    "Number of metrics that has been deleted",
)
UPDATE_READ_ON_METADATA = prometheus_client.Counter(
    "bg_cassandra_update_metric_read_on",
    "Number of 'read_on' that have been updated in metric metadata",
)
INSERT_METRIC_METADATA = prometheus_client.Counter(
    "bg_cassandra_insert_metric_metadata",
    "Number of metrics metadata that has been inserted",
)
TOUCH_METRIC_METADATA = prometheus_client.Counter(
    "bg_cassandra_touch_metric_metadata",
    "Number of metrics metadata that has been touched",
)
UPDATE_METRIC_METADATA = prometheus_client.Counter(
    "bg_cassandra_update_metric_metadata",
    "Number of metrics metadata that has been updated",
)
SELECT_METRIC_METADATA = prometheus_client.Counter(
    "bg_cassandra_select_metric_metadata",
    "Number of metrics metadata that has been selected",
)
INSERT_DIRECTORY = prometheus_client.Counter(
    "bg_cassandra_insert_directory",
    "Number of directories that has been inserted",
)
INSERT_METRIC = prometheus_client.Counter(
    "bg_cassandra_insert_metric",
    "Number of metrics that has been inserted",
)
SELECT_METRIC = prometheus_client.Counter(
    "bg_cassandra_select_metric",
    "Number of metrics that has been selected",
)
SELECT_DIRECTORY = prometheus_client.Counter(
    "bg_cassandra_select_directory",
    "Number of directories that has been selected",
)
SELECT = prometheus_client.Counter(
    "bg_cassandra_select",
    "Number of glob select",
)
METADATA_SCHEMA_UPDATE = prometheus_client.Counter(
    "bg_cassandra_creation_cql_metadata",
    "Number of schema changes that has been executed so far",
)
SELECT_TABLES_FROM_KEYSPACE = prometheus_client.Counter(
    "bg_cassandra_select_tables_from_keyspace",
    "Number of times tables from keyspace have been selected",
)
CLEAN_EXPIRED_METRICS_SELECT = prometheus_client.Counter(
    "bg_cassandra_clean_expired_metrics_select",
    "Number of select to prepare cleaning expired metrics",
)
CLEAN_EXPIRED_METRICS_DELETE = prometheus_client.Counter(
    "bg_cassandra_clean_expired_metrics_delete",
    "Number of expired metrics delete",
)
CLEAN_EXPIRED_METRICS_DELETE_METADATA = prometheus_client.Counter(
    "bg_cassandra_clean_expired_metrics_delete_metadata",
    "Number of expired metrics metadata delete",
)
PM_DELETED_DIRECTORIES = prometheus_client.Counter(
    "bg_cassandra_deleted_directories",
    "Number of directory that have been deleted so far",
)
PM_REPAIRED_DIRECTORIES = prometheus_client.Counter(
    "bg_cassandra_repaired_directories", "Number of missing directory created"
)
PM_EXPIRED_METRICS = prometheus_client.Counter(
    "bg_cassandra_expired_metrics",
    "Number of metrics that has been cleaned due to expiration",
)
INSERT_DOWNSAMPLED_POINTS = prometheus_client.Counter(
    "bg_cassandra_insert_downsampled_points",
    "Number of downsampled point insertion",
)
INSERT_DOWNSAMPLED_POINTS_BATCH = prometheus_client.Counter(
    "bg_cassandra_insert_downsampled_points_batch",
    "Number of batched downsampled point insertion",
)
SYNCDB = prometheus_client.Counter(
    "bg_cassandra_syncdb",
    "Number of requests to syncdb",
)
CLEAN = prometheus_client.Counter(
    "bg_cassandra_clean_empty_dir",
    "Number of directory cleaning",
)
CLEAN_DIRECTORIES_TO_CHECK = prometheus_client.Counter(
    "bg_cassandra_clean_empty_dir_directories_to_check",
    "Number of queries performed to find directories to check during directory cleaning",
)
CLEAN_DIRECTORIES_TO_REMOVE = prometheus_client.Counter(
    "bg_cassandra_clean_empty_dir_directories_to_remove",
    "Number of queries performed to remove directories during directory cleaning",
)
BROKEN_METRICS_TO_CHECK = prometheus_client.Counter(
    "bg_cassandra_repair_broken_metrics_to_check",
    "Number of queries performed to find broken metrics",
)
BROKEN_METRICS_TO_REMOVE = prometheus_client.Counter(
    "bg_cassandra_repair_broken_metrics_to_remove",
    "Number of queries performed to remove broken metrics",
)
REPAIR_MISSING_DIR_COUNT = prometheus_client.Counter(
    "bg_cassandra_repair_missing_dir",
    "Number of missing dir repair",
)
CLEAN_BROKEN_METRICS_COUNT = prometheus_client.Counter(
    "bg_cassandra_repair_broken_metrics",
    "Number of missing broken metrics queries",
)
MAP_ITERATION = prometheus_client.Counter(
    "bg_cassandra_map_iteration",
    "Number of iteration on map so far",
)
DROP_ALL_METRICS = prometheus_client.Counter(
    "bg_cassandra_drop_all_metrics",
    "Number of drop all triggers",
)
FETCH_POINT = prometheus_client.Counter(
    "bg_cassandra_fetch_point",
    "Number of point fetch",
)
REPAIR_DIRECTORIES_TO_CHECK = prometheus_client.Counter(
    "bg_cassandra_repair_directories_to_check",
    "Number of queries to find directories to check during a repair",
)
REPAIR_DIRECTORIES_TO_CREATE = prometheus_client.Counter(
    "bg_cassandra_repair_directories_to_create",
    "Number of queries to create directories  during a repair",
)


SYNCDB_DATA = prometheus_client.Summary(
    "bg_cassandra_syncdb_data_latency_seconds",
    "DB data sync latency in seconds"
)
SYNCDB_METADATA = prometheus_client.Summary(
    "bg_cassandra_syncdb_metadata_latency_seconds",
    "DB metadata sync latency in seconds"
)
CLEAN_EMPTY_DIR = prometheus_client.Summary(
    "bg_cassandra_clean_empty_dir_latency_seconds",
    "clean empty dir latency in seconds"
)
CLEAN_BROKEN_METRICS = prometheus_client.Summary(
    "bg_cassandra_clean_broken_metrics_latency_seconds",
    "clean broken metrics latency in seconds"
)
CLEAN_EXPIRED_METRICS = prometheus_client.Summary(
    "bg_cassandra_clean_expired_metrics_latency_seconds",
    "clean expired metrics latency in seconds"
)
REPAIR_MISSING_DIR = prometheus_client.Summary(
    "bg_cassandra_repair_missing_dir_latency_seconds",
    "repair missing directory latency in seconds"
)
SELECT_METRIC_METADATA_LATENCY = prometheus_client.Summary(
    "bg_cassandra_select_metric_metadata_latency_seconds",
    "select metric metadata latency in seconds"
)
DELETE_DIRECTORY_LATENCY = prometheus_client.Summary(
    "bg_cassandra_delete_directory_latency_seconds",
    "delete directory latency in seconds"
)
HAS_DIRECTORY = prometheus_client.Summary(
    "bg_cassandra_has_directory_latency_seconds",
    "has directory latency in seconds"
)
UPDATE_METRIC = prometheus_client.Summary(
    "bg_cassandra_update_metric_latency_seconds",
    "update_metric latency in seconds"
)

CLEAN_START_TS_METRIC = prometheus_client.Gauge(
    "bg_util_clean_ts_start",
    "clean last start timestamp"
)
CLEAN_STOP_TS_METRIC = prometheus_client.Gauge(
    "bg_util_clean_ts_stop",
    "clean last end timestamp"
)
CLEAN_STEP_TS = prometheus_client.Gauge(
    "bg_util_clean_step_duration",
    "Duration of any step of the cleaning",
    ['step']
)

CLEAN_CURRENT_STEP = prometheus_client.Gauge(
    "bg_util_clean_current_step",
    "clean current step (0: none, 1: expired directories, 2: expired metrics)"
)
CLEAN_CURRENT_OFFSET = prometheus_client.Gauge(
    "bg_util_clean_current_offset",
    "clean current offset"
)
CLEAN_SKIPPED_OFFSET = prometheus_client.Counter(
    "bg_util_clean_skipped_offset",
    "skipped offset while cleaning"
)

ERROR_BIND_METRIC = prometheus_client.Counter(
    "bg_error_bind_metric",
    "Number of incomplete or invalid record read from backend",
    ["type"]
)

log = logging.getLogger(__name__)

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
DEFAULT_READ_ON_SAMPLING_RATE = 0.1
DEFAULT_USE_LUCENE = False
DEFAULT_SSL_VERIFY_MODE = 'required'

# Exceptions that are not considered fatal during batch jobs.
# The affected range will simply be ignored.
BATCH_IGNORED_EXCEPTIONS = (cassandra.cluster.NoHostAvailable, cassandra.ReadTimeout)
BATCH_MAX_IGNORED_ERRORS = 100

DIRECTORY_SEPARATOR = common.DIRECTORY_SEPARATOR

consistency_name_to_value = cassandra.ConsistencyLevel.name_to_value


def _consistency_validator(k):
    if k in consistency_name_to_value.keys():
        return k
    else:
        return None


OPTIONS = {
    "username": str,
    "password": str,
    "keyspace": str,
    "ssl_enable": bool,
    "ssl_verify_locations": lambda k: None if k is None else str(k),
    "ssl_cert_file": lambda k: None if k is None else str(k),
    "ssl_key_file": lambda k: None if k is None else str(k),
    "ssl_verify_mode": str,
    "ssl_check_hostname": lambda k: None if k is None else str(k),
    "ssl_enable_metadata": bool,
    "ssl_verify_locations_metadata": lambda k: None if k is None else str(k),
    "ssl_cert_file_metadata": lambda k: None if k is None else str(k),
    "ssl_key_file_metadata": lambda k: None if k is None else str(k),
    "ssl_verify_mode_metadata": str,
    "ssl_check_hostname_metadata": lambda k: None if k is None else str(k),
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
    "use_lucene": bool,
    # allowed compaction strategies: DateTieredCompactionStrategy or TimeWindowCompactionStrategy
    # None is legacy behaviour, using cassandra version
    "compaction_strategy": lambda k: k if k in ["DateTieredCompactionStrategy", "TimeWindowCompactionStrategy"]
    else None,
}


def add_argparse_arguments(parser):
    """Add Cassandra arguments to an argparse parser."""
    parser.add_argument(
        "--cassandra_keyspace",
        metavar="NAME",
        help="Cassandra keyspace.",
        default=DEFAULT_KEYSPACE,
    )
    parser.add_argument(
        "--cassandra_username",
        help="Cassandra username.",
        default=None
    )
    parser.add_argument(
        "--cassandra_password",
        help="Cassandra password.",
        default=None
    )
    parser.add_argument(
        "--cassandra_ssl_enable",
        help="Cassandra enable SSL",
        default=False,
        action='store_true'
    )
    parser.add_argument(
        "--cassandra_ssl_verify_locations",
        help="Cassandra SSL verify locations (CA path)",
        default=None
    )
    parser.add_argument(
        "--cassandra_ssl_cert_file",
        help="Cassandra SSL certificate file",
        default=None
    )
    parser.add_argument(
        "--cassandra_ssl_key_file",
        help="Cassandra SSL certificate's key file",
        default=None
    )
    parser.add_argument(
        "--cassandra_ssl_verify_mode",
        help="Cassandra SSL verify mode (none, required)",
        type=str,
        default=DEFAULT_SSL_VERIFY_MODE,
    )
    parser.add_argument(
        "--cassandra_ssl_check_hostname",
        help="Cassandra SSL hostname to check",
        default=None
    )
    parser.add_argument(
        "--cassandra_ssl_enable_metadata",
        help="Cassandra enable SSL (metadata)",
        default=False,
        action='store_true',
    )
    parser.add_argument(
        "--cassandra_ssl_verify_locations_metadata",
        help="Cassandra SSL verify locations (CA path, for metadata)",
        default=None
    )
    parser.add_argument(
        "--cassandra_ssl_cert_file_metadata",
        help="Cassandra SSL certificate file (metadata)",
        default=None
    )
    parser.add_argument(
        "--cassandra_ssl_key_file_metadata",
        help="Cassandra SSL certificate's key file (metadata)",
        default=None
    )
    parser.add_argument(
        "--cassandra_ssl_verify_mode_metadata",
        help="Cassandra SSL verify mode (none, required)",
        type=str,
        default=DEFAULT_SSL_VERIFY_MODE,
    )
    parser.add_argument(
        "--cassandra_ssl_check_hostname_metadata",
        help="Cassandra SSL hostname to check",
        default=None
    )
    parser.add_argument(
        "--cassandra_contact_points",
        metavar="HOST[,HOST,...]",
        help="Hosts used for discovery.",
        default=DEFAULT_CONTACT_POINTS,
    )
    parser.add_argument(
        "--cassandra_concurrent_connections",
        metavar="N",
        type=int,
        help="Maximum concurrent connections to the cluster.",
        default=DEFAULT_MAX_CONCURRENT_CONNECTIONS,
    )
    parser.add_argument(
        "--cassandra_contact_points_metadata",
        metavar="HOST[,HOST,...]",
        help="Hosts used for discovery.",
        default=None,
    )
    parser.add_argument(
        "--cassandra_port",
        metavar="PORT",
        type=int,
        help="The native port to connect to.",
        default=DEFAULT_PORT,
    )
    parser.add_argument(
        "--cassandra_port_metadata",
        metavar="PORT",
        type=int,
        help="The native port to connect to.",
        default=None,
    )
    parser.add_argument(
        "--cassandra_timeout",
        metavar="TIMEOUT",
        type=int,
        help="Cassandra query timeout in seconds.",
        default=DEFAULT_TIMEOUT,
    )
    parser.add_argument(
        "--cassandra_compression",
        metavar="COMPRESSION",
        type=str,
        help="Cassandra network compression.",
        default=DEFAULT_COMPRESSION,
    )
    parser.add_argument(
        "--cassandra_max_metrics_per_pattern",
        help="Maximum number of metrics returned for a glob query.",
        default=DEFAULT_MAX_METRICS_PER_PATTERN,
    )
    parser.add_argument(
        "--cassandra_max_queries_per_pattern",
        type=int,
        help="Maximum number of sub-queries for any pattern-based query.",
        default=DEFAULT_MAX_QUERIES_PER_PATTERN,
    )
    parser.add_argument(
        "--cassandra_max_concurrent_queries_per_pattern",
        type=int,
        help="Maximum number of concurrently executed sub-queries for any pattern-based query.",
        default=DEFAULT_MAX_CONCURRENT_QUERIES_PER_PATTERN,
    )
    parser.add_argument(
        "--cassandra_trace",
        help="Enable query traces",
        default=DEFAULT_TRACE,
        action="store_true",
    )
    parser.add_argument(
        "--cassandra_bulkimport",
        action="store_true",
        help="Generate files needed for bulkimport.",
    )
    parser.add_argument(
        "--cassandra_enable_metrics",
        help="should expose metrics",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "--cassandra_writer", type=int, help="Cassandra writer", default=None
    )
    parser.add_argument(
        "--cassandra_replica", type=int, help="Cassandra replica", default=None
    )
    parser.add_argument(
        "--cassandra_meta_write_consistency",
        metavar="META_WRITE_CONS",
        help="Metadata write consistency",
        default=DEFAULT_META_WRITE_CONSISTENCY,
    )
    parser.add_argument(
        "--cassandra_meta_read_consistency",
        metavar="META_READ_CONS",
        help="Metadata read consistency",
        default=DEFAULT_META_READ_CONSISTENCY,
    )
    parser.add_argument(
        "--cassandra_meta_serial_consistency",
        metavar="META_SERIAL_CONS",
        help="Metadata serial consistency",
        default=DEFAULT_META_SERIAL_CONSISTENCY,
    )
    parser.add_argument(
        "--cassandra_meta_background_consistency",
        metavar="META_BACKGROUND_CONS",
        help="Metadata background consistency",
        default=DEFAULT_META_BACKGROUND_CONSISTENCY,
    )
    parser.add_argument(
        "--cassandra_data_write_consistency",
        metavar="DATA_WRITE_CONS",
        help="Data write consistency",
        default=DEFAULT_DATA_WRITE_CONSISTENCY,
    )
    parser.add_argument(
        "--cassandra_data_read_consistency",
        metavar="DATA_READ_CONS",
        help="Data read consistency",
        default=DEFAULT_DATA_READ_CONSISTENCY,
    )
    parser.add_argument(
        "--cassandra_updated_on_ttl_sec",
        help="Update 'updated_on' field every x seconds.",
        default=DEFAULT_UPDATED_ON_TTL_SEC,
    )
    parser.add_argument(
        "--cassandra_read_on_sampling_rate",
        help="Updated 'read_on' field every x calls.",
        default=DEFAULT_READ_ON_SAMPLING_RATE,
    ),
    parser.add_argument(
        "--cassandra_use_lucene",
        help="Use the Stratio Lucene Index.",
        default=DEFAULT_USE_LUCENE,
    )
    parser.add_argument(
        "--cassandra_compaction_strategy",
        help="Cassandra compaction strategy",
        type=str,
        default=None,
    )


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

_COMPONENTS_MAX_LEN = common.COMPONENTS_MAX_LEN
_LAST_COMPONENT = common.LAST_COMPONENT
_METADATA_CREATION_CQL_PATH_COMPONENTS = ", ".join(
    "component_%d text" % n for n in range(_COMPONENTS_MAX_LEN)
)

_METADATA_CREATION_CQL_METRICS_METADATA = str(
    'CREATE TABLE IF NOT EXISTS "%(keyspace)s".metrics_metadata ('
    "  name text,"
    "  created_on  timeuuid,"
    "  updated_on  timeuuid,"
    "  read_on  timeuuid,"
    "  id uuid,"
    "  config map<text, text>,"
    "  PRIMARY KEY ((name))"
    ");"
)

_METADATA_CREATION_CQL_METRICS_METADATA_CREATED_ON_INDEX = [
    'CREATE CUSTOM INDEX IF NOT EXISTS ON "%%(keyspace)s".%(table)s (created_on)'
    "  USING 'org.apache.cassandra.index.sasi.SASIIndex'"
    "  WITH OPTIONS = {"
    "    'mode': 'SPARSE'"
    "  };" % {"table": "metrics_metadata"}
]

_METADATA_CREATION_CQL_METRICS_METADATA_UPDATED_ON_INDEX = [
    'CREATE CUSTOM INDEX IF NOT EXISTS ON "%%(keyspace)s".%(table)s (updated_on)'
    "  USING 'org.apache.cassandra.index.sasi.SASIIndex'"
    "  WITH OPTIONS = {"
    "    'mode': 'SPARSE'"
    "  };" % {"table": "metrics_metadata"}
]

_METADATA_CREATION_CQL_METRICS_METADATA_READ_ON_INDEX = [
    'CREATE CUSTOM INDEX IF NOT EXISTS ON "%%(keyspace)s".%(table)s (read_on)'
    "  USING 'org.apache.cassandra.index.sasi.SASIIndex'"
    "  WITH OPTIONS = {"
    "    'mode': 'SPARSE'"
    "  };" % {"table": "metrics_metadata"}
]

_METADATA_CREATION_CQL_METRICS = str(
    'CREATE TABLE IF NOT EXISTS "%(keyspace)s".metrics ('
    "  name text,"
    "  parent text,"
    "  " + _METADATA_CREATION_CQL_PATH_COMPONENTS + ","
    "  PRIMARY KEY (name)"
    ");"
)
_METADATA_CREATION_CQL_DIRECTORIES = str(
    'CREATE TABLE IF NOT EXISTS "%(keyspace)s".directories ('
    "  name text,"
    "  parent text,"
    "  " + _METADATA_CREATION_CQL_PATH_COMPONENTS + ","
    "  PRIMARY KEY (name)"
    ");"
)
_METADATA_CREATION_CQL_ID_INDEXES = [
    'CREATE CUSTOM INDEX IF NOT EXISTS ON "%%(keyspace)s".%(table)s (id)'
    "  USING 'org.apache.cassandra.index.sasi.SASIIndex'"
    "  WITH OPTIONS = {"
    "    'mode': 'SPARSE'"
    "  };" % {"table": "metrics_metadata"}
]
_METADATA_CREATION_CQL = (
    [
        _METADATA_CREATION_CQL_METRICS,
        _METADATA_CREATION_CQL_DIRECTORIES,
        _METADATA_CREATION_CQL_METRICS_METADATA,
    ]
    + _METADATA_CREATION_CQL_ID_INDEXES
    + _METADATA_CREATION_CQL_METRICS_METADATA_CREATED_ON_INDEX
    + _METADATA_CREATION_CQL_METRICS_METADATA_UPDATED_ON_INDEX
    + _METADATA_CREATION_CQL_METRICS_METADATA_READ_ON_INDEX
    # FIXME: Use the correct index in clean/repair
    + sasi.METADATA_CREATION_CQL_PARENT_INDEXES
)

_METADATA_CREATION_CQL_SASI = sasi.METADATA_CREATION_CQL_PATH_INDEXES
GLOBSTAR = bg_glob.Globstar()

_METADATA_CREATION_CQL_LUCENE = lucene.CQL_CREATE_INDICES

_DATAPOINTS_CREATION_CQL_TEMPLATE = str(
    "CREATE TABLE IF NOT EXISTS %(table)s ("
    "  metric uuid,"  # Metric UUID.
    "  time_start_ms bigint,"  # Lower bound for this row.
    "  offset smallint,"  # time_start_ms + offset * precision = timestamp
    "  shard  smallint,"  # Writer shard to allow restarts.
    "  value double,"  # Value for the point.
    # If value is sum, divide by count to get the avg.
    "  count smallint,"
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
    "  metric uuid,"  # Metric UUID.
    "  time_start_ms bigint,"  # Lower bound for this row.
    "  offset smallint,"  # time_start_ms + offset * precision = timestamp
    "  value double,"  # Value for the point.
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
    "DateTieredCompactionStrategy": str(
        "    'base_time_seconds': '%(base_time_seconds)d',"
        "    'max_window_size_seconds': %(max_window_size_seconds)d"
    ),
    "TimeWindowCompactionStrategy": str(
        "    'compaction_window_unit': '%(compaction_window_unit)s',"
        "    'compaction_window_size': %(compaction_window_size)d"
    ),
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
        max(stage.precision_ms * _EXPECTED_POINTS_PER_READ, _MIN_PARTITION_SIZE_MS),
    )
    return bg_utils.round_up(row_size_ms, _ROW_SIZE_PRECISION_MS)


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
        # TODO: explain why we need that. I think it's to limit memory usage.
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

    def __init__(
        self,
        session,
        keyspace,
        shard,
        bulkimport,
        data_write_consistency,
        data_read_consistency,
        compaction_strategy=None
    ):
        self._keyspace = keyspace
        self._session = session
        self._bulkimport = bulkimport
        self.__stage_to_insert = {}
        self.__stage_to_select = {}
        self.__data_files = {}
        self._shard = shard
        self._data_write_consistency = data_write_consistency
        self._data_read_consistency = data_read_consistency
        self._COMPACTION_STRATEGY = compaction_strategy
        if not compaction_strategy:
            release_version = list(session.get_pools())[0].host.release_version
            if version.LooseVersion(release_version) >= version.LooseVersion("3.9"):
                self._COMPACTION_STRATEGY = "TimeWindowCompactionStrategy"
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

        cs_template = _DATAPOINTS_CREATION_CQL_CS_TEMPLATE.get(
            self._COMPACTION_STRATEGY
        )
        if not cs_template:
            raise InvalidArgumentError(
                "Unknown compaction strategy '%s'" % self._COMPACTION_STRATEGY
            )
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
                    fresh_time,
                ),
            )

            # Make it readable.
            if window_size > DAY:
                unit = "DAYS"
                window_size /= DAY
            else:
                unit = "HOURS"
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
        return '"{}"."datapoints_{}p_{}s{}"'.format(
            self._keyspace, stage.points, stage.precision, suffix
        )

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

    def prepare_select(
        self, stage, metric_id, row_start_ms, row_min_offset, row_max_offset
    ):
        # Don't set any limit. We have no idea how much we should limit since
        # we don't know how many writers were used. In addition there already
        # are Cassandra server-side protection like timeout and paging.
        args = (metric_id, row_start_ms, row_min_offset, row_max_offset)

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
            " ORDER BY offset;"
        ) % {"columns": ", ".join(columns), "table": self._get_table_name(stage)}
        statement = self._session.prepare(statement_str)
        statement.consistency_level = self._data_read_consistency
        self.__stage_to_select[stage] = statement
        return statement, args


def expose_metrics(metrics, cluster_name=""):
    """Adaptor to notify prometheus of Cassandra's metrics change."""
    metrics_adp = {}

    def counter_adaptor(cpt, fn):
        def inner(*args, **kwargs):
            cpt.inc()
            fn(*args, **kwargs)

        return inner

    for attr in dir(metrics):
        if attr.startswith("on_"):
            metric_name = "bg_cassandra_" + cluster_name + "_" + attr[3:]
            cpt = prometheus_client.Counter(metric_name, "")
            metrics_adp[metric_name] = cpt
            setattr(metrics, attr, counter_adaptor(cpt, attr))

    return metrics_adp


class _CassandraExecutionRequest(object):
    """Wraps required data for a Cassandra query."""

    def __init__(self, counter, statement, *params):
        """Inits the execution request."""
        self.counter = counter
        self.statement = statement
        self.params = params

    def mark(self):
        """Mark this context as executed."""
        if self.counter:
            self.counter.inc()

    def args(self):
        """Returns the query arguments as expected by the Cassandra driver."""
        return self.statement, list(self.params)

    def with_params(self, *params):
        """Clone this execution request with parameters."""
        return _CassandraExecutionRequest(self.counter, self.statement, *params)

    def with_param_list(self, param_list):
        """Clone this execution request with parameters as list."""
        return self.with_params(*tuple(param_list))

    def __repr__(self):
        return str(self.statement)


class _CassandraAccessor(bg_accessor.Accessor):
    """Provides Read/Write accessors to Cassandra.

    Please refer to bg_accessor.Accessor.
    """

    TYPE = "cassandra"

    def __init__(
        self,
        keyspace=DEFAULT_KEYSPACE,
        username=None,
        password=None,
        ssl_enable=False,
        ssl_verify_locations=None,
        ssl_cert_file=None,
        ssl_key_file=None,
        ssl_verify_mode=DEFAULT_SSL_VERIFY_MODE,
        ssl_check_hostname=None,
        ssl_enable_metadata=False,
        ssl_verify_locations_metadata=None,
        ssl_cert_file_metadata=None,
        ssl_key_file_metadata=None,
        ssl_verify_mode_metadata=DEFAULT_SSL_VERIFY_MODE,
        ssl_check_hostname_metadata=None,
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
        read_on_sampling_rate=DEFAULT_READ_ON_SAMPLING_RATE,
        use_lucene=DEFAULT_USE_LUCENE,
        enable_metadata=True,
        compaction_strategy=None
    ):
        """Record parameters needed to connect.

        Args:
          keyspace: Base names of Cassandra keyspaces dedicated to BigGraphite.
          contact_points: list of strings, the hostnames or IP to use to discover Cassandra.
          port: The port to connect to, as an int.
          contact_points_metadata: list of strings, the hostnames or IP to use
            to discover Cassandra.
          port_metadata: The port to connect to, as an int.
          ssl_enable: bool, Enable SSL to cassandra.
          ssl_verify_locations: string, Path to CA certificate.
          ssl_cert_file: string, Path to SSL client certificate.
          ssl_key_file: string, Path to SSL key certificate.
          ssl_verify_mode: string, Verify mode (none or required).
          ssl_check_hostname: string, SSL target hostname to check.
          ssl_enable_metadata: bool, Enable SSL to cassandra (metadata).
          ssl_verify_locations_metadata: string, Path to CA certificate (metadata).
          ssl_cert_file_metadata: string, Path to SSL client certificate (metadata).
          ssl_key_file_metadata: string, Path to SSL key certificate (metadata).
          ssl_verify_mode_metadata: string, Verify mode (none or required) (metadata).
          ssl_check_hostname_metadata: string, SSL target hostname to check (metadata).
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
          enable_metadata: bool, whether if the driver should handle the metadata or not
          enable_metrics: bool, enable metrics
          data_read_consistency: string, Default consistency to use for data reads
          data_write_consistency: string, Default consistency to use for data writes
          max_concurrent_connections: int, Maximum number of Cassandra connections
          meta_background_consistency: string, Default consistency for metadata reads & writes
            in background
          meta_read_consistency: string, Default consistency for metadata reads
          meta_serial_consistency: string, Default consistency for serial operations
          meta_write_consistency: string, Default consistency for metadata writes
          username: string, Cassandra connection username to use
          password: string, Cassandra connection password to use
          read_on_sampling_rate: int, Read_on update frequency (per call)
          updated_on_ttl_sec: int, Updated_on update frequency (per seconds)
          use_lucene: bool, Use the Stratio Lucene Index
          compaction_strategy: string or None, compaction strategy to use, if not, it will use a default depending
                               on cassandra version.
        """
        backend_name = "cassandra:" + keyspace
        super(_CassandraAccessor, self).__init__(backend_name)

        if not contact_points_metadata:
            contact_points_metadata = contact_points
        if not port_metadata:
            port_metadata = port

        self.keyspace = keyspace
        self.keyspace_metadata = keyspace + "_metadata"
        if username is not None and password is not None:
            self.auth_provider = auth.PlainTextAuthProvider(username, password)
        else:
            self.auth_provider = None

        self.__ssl_enable = ssl_enable
        self.__ssl_enable_metadata = ssl_enable_metadata

        self.__ssl_verify_locations = ssl_verify_locations
        self.__ssl_cert_file = ssl_cert_file
        self.__ssl_key_file = ssl_key_file

        self.__ssl_verify_mode = ssl_verify_mode
        self.__ssl_check_hostname = ssl_check_hostname
        self.__ssl_cert_file = ssl_cert_file
        self.__ssl_key_file = ssl_key_file

        self.__ssl_verify_locations_metadata = ssl_verify_locations_metadata
        self.__ssl_cert_file_metadata = ssl_cert_file_metadata
        self.__ssl_key_file_metadata = ssl_key_file_metadata
        self.__ssl_verify_mode_metadata = ssl_verify_mode_metadata
        self.__ssl_check_hostname_metadata = ssl_check_hostname_metadata

        self.contact_points_data = contact_points
        self.contact_points_metadata = contact_points_metadata
        self.port = port
        self.port_metadata = port_metadata
        self.use_lucene = use_lucene
        if use_lucene:
            self.TYPE += "-lucene"
        self.max_metrics_per_pattern = max_metrics_per_pattern
        self.max_queries_per_pattern = max_queries_per_pattern
        self.max_concurrent_queries_per_pattern = max_concurrent_queries_per_pattern
        self.max_concurrent_connections = max_concurrent_connections
        self.__compression = compression
        self.__trace = trace
        self.__bulkimport = bulkimport
        self.__metadata_touch_ttl_sec = updated_on_ttl_sec
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
        self._meta_serial_consistency = consistency_name_to_value[
            meta_serial_consistency
        ]
        self._meta_background_consistency = consistency_name_to_value[
            meta_background_consistency
        ]
        self._data_write_consistency = consistency_name_to_value[data_write_consistency]
        self._data_read_consistency = consistency_name_to_value[data_read_consistency]
        self.__compaction_strategy = compaction_strategy
        if writer is None:
            # TODO: Currently a random shard is good enough.
            # We should use a counter stored in cassandra instead.
            writer = bg_accessor.pack_shard(
                replica, random.getrandbits(bg_accessor.SHARD_WRITER_BITS)
            )
        # Cassandra expects a signed short, make sure we give it something
        # it understands.
        self.__shard = self.__shard = bg_accessor.pack_shard(replica, writer)
        self.__shard = c_marshal.int16_unpack(c_marshal.uint16_pack(self.__shard))

        if self.use_lucene:
            self.metadata_query_generator = lucene.CassandraStratioLucene(
                self.keyspace_metadata,
                self.max_queries_per_pattern,
                self.max_metrics_per_pattern,
            )
        else:
            self.metadata_query_generator = sasi.CassandraSASI(
                self.keyspace_metadata,
                self.max_queries_per_pattern,
                self.max_metrics_per_pattern,
            )

        self.metadata_enabled = enable_metadata

    @tracing.trace
    def connect(self):
        """See bg_accessor.Accessor."""
        super(_CassandraAccessor, self).connect()
        if not self.is_connected:
            self._connect_clusters()
            self._prepare_metadata_statements()
        self.is_connected = True

    @property
    def shard(self):
        return self.__shard

    @shard.setter
    def shard(self, value):
        self.__shard = value
        if self.__lazy_statements:
            self.__lazy_statements._shard = value

    def _prepare_metadata_statements(self):
        if not self.metadata_enabled:
            return

        def __prepare(cql, consistency=self._meta_write_consistency):
            statement = self.__session_metadata.prepare(cql)
            statement.consistency_level = consistency
            return statement

        # Metadata (metrics and directories)
        components_names = ", ".join(
            "component_%d" % n for n in range(_COMPONENTS_MAX_LEN)
        )
        components_marks = ", ".join("?" for n in range(_COMPONENTS_MAX_LEN))
        self.__insert_metric_statement = _CassandraExecutionRequest(
            INSERT_METRIC,
            __prepare(
                'INSERT INTO "%s".metrics (name, parent, %s) VALUES (?, ?, %s);'
                % (self.keyspace_metadata, components_names, components_marks)
            )
        )
        self.__insert_directory_statement = _CassandraExecutionRequest(
            INSERT_DIRECTORY,
            __prepare(
                'INSERT INTO "%s".directories (name, parent, %s) VALUES (?, ?, %s) IF NOT EXISTS;'
                % (self.keyspace_metadata, components_names, components_marks)
            )
        )
        self.__insert_directory_statement.statement.serial_consistency_level = (
            self._meta_serial_consistency
        )
        # We do not set the serial_consistency, it defautls to SERIAL.
        self.__select_metric_metadata_statement = _CassandraExecutionRequest(
            SELECT_METRIC_METADATA,
            __prepare(
                "SELECT id, config, toUnixTimestamp(updated_on), name"
                ' FROM "%s".metrics_metadata WHERE name = ?;' % self.keyspace_metadata,
                self._meta_read_consistency,
            )
        )
        self.__select_metric_statement = __prepare(
            'SELECT * FROM "%s".metrics WHERE name = ?;' % self.keyspace_metadata,
            self._meta_read_consistency,
        )
        self.__select_directory_statement = __prepare(
            'SELECT * FROM "%s".directories WHERE name = ?;' % self.keyspace_metadata,
            self._meta_read_consistency,
        )
        self.__update_metric_metadata_statement = _CassandraExecutionRequest(
            UPDATE_METRIC_METADATA,
            __prepare(
                'UPDATE "%s".metrics_metadata SET config=?, updated_on=now()'
                " WHERE name=?;" % self.keyspace_metadata
            )
        )
        self.__touch_metrics_metadata_statement = _CassandraExecutionRequest(
            TOUCH_METRIC_METADATA,
            __prepare(
                'UPDATE "%s".metrics_metadata SET updated_on=now()'
                " WHERE name=?;" % self.keyspace_metadata
            ))
        self.__insert_metrics_metadata_statement = _CassandraExecutionRequest(
            INSERT_METRIC_METADATA,
            __prepare(
                'INSERT INTO "%s".metrics_metadata (name, created_on, updated_on, id, config)'
                " VALUES (?, now(), now(), ?, ?);" % self.keyspace_metadata
            )
        )
        self.__update_metric_read_on_metadata_statement = _CassandraExecutionRequest(
            UPDATE_READ_ON_METADATA,
            __prepare(
                'UPDATE "%s".metrics_metadata SET read_on=now()'
                " WHERE name=? IF EXISTS;" % self.keyspace_metadata
            )
        )
        self.__delete_metric = _CassandraExecutionRequest(
            DELETE_METRIC,
            __prepare(
                'DELETE FROM "%s".metrics WHERE name=?;' % (self.keyspace_metadata),
                consistency=cassandra.ConsistencyLevel.QUORUM,
            )
        )
        self.__delete_directory = _CassandraExecutionRequest(
            DELETE_DIRECTORY,
            __prepare(
                'DELETE FROM "%s".directories WHERE name=?;' % (self.keyspace_metadata),
                consistency=cassandra.ConsistencyLevel.QUORUM,
            )
        )
        self.__delete_metric_metadata = _CassandraExecutionRequest(
            DELETE_METRIC_METADATA,
            __prepare(
                'DELETE FROM "%s".metrics_metadata WHERE name=?;'
                % (self.keyspace_metadata),
                consistency=cassandra.ConsistencyLevel.QUORUM,
            )
        )

    def _get_ssl_context(self):
        if self.__ssl_enable is False:
            return None, None

        ssl_context = SSLContext(PROTOCOL_TLS)
        ssl_context.load_default_certs()

        if self.__ssl_verify_mode == 'required':
            ssl_context.verify_mode = CERT_REQUIRED
        else:
            ssl_context.verify_mode = CERT_NONE

        ssl_options = None

        if self.__ssl_check_hostname is not None:
            ssl_context.check_hostname = True
            ssl_options = {'server_hostname': self.__ssl_check_hostname}

        if self.__ssl_verify_locations is not None:
            ssl_context.load_verify_locations(
                self.__ssl_verify_locations
            )

        if self.__ssl_cert_file is not None and self.__ssl_key_file is not None:
            ssl_context.load_cert_chain(
                certfile=self.__ssl_cert_file,
                keyfile=self.__ssl_key_file
            )

        return ssl_context, ssl_options

    def _get_ssl_context_metadata(self):
        if self.__ssl_enable_metadata is False:
            return None, None

        ssl_context = SSLContext(PROTOCOL_TLS)
        ssl_context.load_default_certs()

        if self.__ssl_verify_mode_metadata == 'required':
            ssl_context.verify_mode = CERT_REQUIRED
        else:
            ssl_context.verify_mode = CERT_NONE

        ssl_options = None

        if self.__ssl_check_hostname_metadata is not None:
            ssl_context.check_hostname = True
            ssl_options = {'server_hostname': self.__ssl_check_hostname_metadata}

        if self.__ssl_verify_locations_metadata is not None:
            ssl_context.load_verify_locations(
                self.__ssl_verify_locations_metadata
            )

        if self.__ssl_cert_file_metadata is not None and self.__ssl_key_file_metadata is not None:
            ssl_context.load_cert_chain(
                certfile=self.__ssl_cert_file_metadata,
                keyfile=self.__ssl_key_file_metadata
            )

        return ssl_context, ssl_options

    def _connect_clusters(self):
        # data cluster
        ssl_context, ssl_options = self._get_ssl_context()
        self.__cluster_data, self.__session_data = self._connect(
            self.__cluster_data,
            self.__session_data,
            self.contact_points_data,
            self.port,
            ssl_context,
            ssl_options,
        )
        self.__lazy_statements = _LazyPreparedStatements(
            self.__session_data,
            self.keyspace,
            self.__shard,
            self.__bulkimport,
            self._meta_write_consistency,
            self._data_read_consistency,
            self.__compaction_strategy
        )
        if self.__enable_metrics:
            self.__metrics["data"] = expose_metrics(self.__cluster_data.metrics, "data")

        # metadata cluster
        if self.metadata_enabled:
            if self.contact_points_data != self.contact_points_metadata:
                ssl_context_metadata, ssl_options_metadata = self._get_ssl_context_metadata()
                self.__cluster_metadata, self.__session_metadata = self._connect(
                    self.__cluster_metadata,
                    self.__session_metadata,
                    self.contact_points_metadata,
                    self.port_metadata,
                    ssl_context_metadata,
                    ssl_options_metadata,
                )
            else:
                self.__session_metadata = self.__session_data
                self.__cluster_metadata = self.__cluster_data

            if self.__enable_metrics:
                self.__metrics["metadata"] = expose_metrics(
                    self.__cluster_metadata.metrics, "metadata"
                )

    def _connect(self, cluster, session, contact_points, port, ssl_context=None, ssl_options=None):
        lb_policy = c_policies.TokenAwarePolicy(c_policies.DCAwareRoundRobinPolicy())
        # See https://datastax-oss.atlassian.net/browse/PYTHON-643
        lb_policy.shuffle_replicas = True

        if not cluster:
            cluster = c_cluster.Cluster(
                contact_points,
                port,
                compression=self.__compression,
                auth_provider=self.auth_provider,
                # Metrics are disabled because too expensive to compute.
                metrics_enabled=False,
                load_balancing_policy=lb_policy,
                ssl_context=ssl_context,
                ssl_options=ssl_options,
            )

            # Limits in flight requests
            cluster.connection_class = getConnectionClass()

        if session:
            session.shutdown()

        session = cluster.connect()
        session.row_factory = c_query.tuple_factory  # Saves 2% CPU
        if self.__timeout:
            session.default_timeout = self.__timeout
        return cluster, session

    def _execute(self, session, execution_request, **kwargs):
        """Wrapper for __session.execute_async()."""
        if self.__bulkimport:
            return []

        if self.__trace:
            kwargs["trace"] = True

        args = execution_request.args()

        if args:
            log.debug(" ".join([str(arg) for arg in args]))
        if kwargs:
            log.debug(" ".join(["%s:%s " % (k, v) for k, v in kwargs.items()]))
        execution_request.mark()
        result = session.execute(*args, **kwargs)

        if self.__trace:
            trace = result.get_query_trace()
            for e in trace.events:
                log.debug("%s: %s" % (e.source_elapsed, str(e)))
        return result

    def _execute_data(self, timer, execution_request, **kwargs):
        with timer.time():
            return self._execute(self.__session_data, execution_request, **kwargs)

    def _execute_metadata(self, timer, execution_request, **kwargs):
        with timer.time():
            return self._execute(self.__session_metadata, execution_request, **kwargs)

    def _execute_async(self, session, execution_request, **kwargs):
        """Wrapper for __session.execute_async()."""
        if self.__bulkimport:

            class _FakeFuture(object):
                def add_callbacks(self, on_result, on_failure):
                    on_result(None)

            return _FakeFuture()

        if self.__trace:
            kwargs["trace"] = True

        if execution_request.params:
            log.debug(" ".join([str(arg) for arg in execution_request.args()]))
        if kwargs:
            log.debug(" ".join(["%s:%s " % (k, v) for k, v in kwargs.items()]))

        execution_request.mark()
        future = session.execute_async(*execution_request.args(), **kwargs)

        if self.__trace:
            trace = future.get_query_trace()
            for e in trace.events:
                log.debug(e.source_elapsed, e.description)
        return future

    def _execute_async_data(self, execution_request, **kwargs):
        return self._execute_async(self.__session_data, execution_request, **kwargs)

    def _execute_async_metadata(self, execution_request, **kwargs):
        return self._execute_async(self.__session_metadata, execution_request, **kwargs)

    def _execute_concurrent(self, session, execution_requests, **kwargs):
        """Wrapper for concurrent.execute_concurrent()."""
        def marked_args(request):
            # Hackish but this is la last checkpoint where we can mark
            # the execution request as executed
            request.mark()
            return request.args()

        if self.__bulkimport:
            return []

        log.debug(execution_requests)

        if not self.__trace:
            args = [marked_args(ec) for ec in execution_requests]
            return c_concurrent.execute_concurrent(
                session, args, **kwargs
            )

        query_results = []
        for execution_request in execution_requests:
            try:
                result = self._execute(session, execution_request, trace=True)
                success = True
            except Exception as e:
                if kwargs.get("raise_on_first_error") is True:
                    raise e
                result = e
                success = False
            query_results.append((success, result))
        return query_results

    def _execute_concurrent_data(self, execution_requests, **kwargs):
        return self._execute_concurrent(self.__session_data, execution_requests, **kwargs)

    def _execute_concurrent_metadata(self, execution_requests, **kwargs):
        return self._execute_concurrent(self.__session_metadata, execution_requests, **kwargs)

    def create_metric(self, metric):
        """See bg_accessor.Accessor."""
        super(_CassandraAccessor, self).create_metric(metric)

        if self.__bulkimport:
            return

        components = self._components_from_name(metric.name)
        assert metric.id is not None and metric.metadata is not None

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
        queries.append(
            (
                self.__insert_metric_statement.with_param_list(
                    [metric.name, parent_dir + "."] + components + padding
                )
            )
        )
        queries.append(
            (
                self.__insert_metrics_metadata_statement.with_param_list(
                    [metric.name, metric.id, metadata_dict]
                )
            )
        )

        self._execute_concurrent_metadata(queries, raise_on_first_error=False)

    def update_metric(self, name, updated_metadata):
        """See bg_accessor.Accessor."""
        super(_CassandraAccessor, self).update_metric(name, updated_metadata)

        if not self.has_metric(name):
            raise InvalidArgumentError("Unknown metric '%s'" % name)

        # Cleanup name (avoid double dots)
        name = ".".join(self._components_from_name(name)[:-1])

        encoded_metric_name = bg_metric.encode_metric_name(name)
        metadata_dict = updated_metadata.as_string_dict()
        self._execute_metadata(
            UPDATE_METRIC,
            self.__update_metric_metadata_statement.with_param_list(
                [metadata_dict, encoded_metric_name]
            )
        )

    def delete_metric(self, name):
        """See bg_accessor.Accessor."""
        super(_CassandraAccessor, self).delete_metric(name)
        self._execute_async_metadata(
            self.__delete_metric.with_params(name)
        )
        self._execute_async_metadata(
            self.__delete_metric_metadata.with_params(name)
        )

    def delete_directory(self, directory):
        """See bg_accessor.Accessor."""
        super(_CassandraAccessor, self).delete_directory(directory)
        self._execute_metadata(
            DELETE_DIRECTORY_LATENCY,
            self.__delete_directory.with_params(directory)
        )

    def _create_parent_dirs_queries(self, components):
        queries = []
        path = []
        parent = ""
        # Incrementally construct parents, till we reach the leaf component.
        # -2 skips leaf (metric name) and _LAST_COMPONENT marker.
        for component in components[:-2]:
            path.append(component)
            name = DIRECTORY_SEPARATOR.join(path)
            path_components = path + [_LAST_COMPONENT]
            padding_len = _COMPONENTS_MAX_LEN - len(path_components)
            padding = [c_query.UNSET_VALUE] * padding_len
            queries.append(
                (
                    self.__insert_directory_statement.with_param_list(
                        [name, parent + DIRECTORY_SEPARATOR] + path_components + padding
                    )
                )
            )
            parent = name

        return queries

    @staticmethod
    def _components_from_name(metric_name):
        res = metric_name.split(".")
        res.append(_LAST_COMPONENT)
        return list(filter(None, res))

    def drop_all_metrics(self):
        """See bg_accessor.Accessor."""
        super(_CassandraAccessor, self).drop_all_metrics()

        def drop_all_metrics_in_keyspace(keyspace, session):
            statement_str = (
                "SELECT table_name FROM system_schema.tables WHERE keyspace_name = %s;"
            )

            select_request = _CassandraExecutionRequest(
                SELECT_TABLES_FROM_KEYSPACE,
                statement_str,
                keyspace)
            tables = [r[0] for r in self._execute(session, select_request)]

            for table in tables:
                self._execute(
                    session,
                    _CassandraExecutionRequest(
                        DROP_ALL_METRICS,
                        'TRUNCATE "%s"."%s";' % (keyspace, table)
                    )
                )

        drop_all_metrics_in_keyspace(self.keyspace, self.__session_data)
        if self.metadata_enabled:
            drop_all_metrics_in_keyspace(self.keyspace_metadata, self.__session_metadata)

        if self.__downsampler:
            self.__downsampler.clear()
        if self.__delayed_writer:
            self.__delayed_writer.clear()

    @tracing.trace
    def fetch_points(self, metric, time_start, time_end, stage, aggregated=True):
        """See bg_accessor.Accessor."""
        super(_CassandraAccessor, self).fetch_points(
            metric, time_start, time_end, stage
        )
        tracing.add_attr_to_trace("metric.name", metric.name)

        if self.metadata_enabled:
            self._update_metric_read_on(metric.name)

        log.debug(
            "fetch: [%s, start=%d, end=%d, stage=%s]",
            metric.name,
            time_start,
            time_end,
            stage,
        )

        time_start_ms = int(time_start) * 1000
        time_end_ms = int(time_end) * 1000
        time_start_ms = max(time_end_ms - stage.duration_ms, time_start_ms)

        execution_requests = self._fetch_points_make_selects(
            metric.id, time_start_ms, time_end_ms, stage
        )
        # Note: it's unclear if this returns as soon as the first
        # query has been executed or not. If yes, consider using execute_async
        # directly.
        query_results = self._execute_concurrent_data(
            execution_requests, results_generator=True
        )

        return bg_accessor.PointGrouper(
            metric,
            time_start_ms,
            time_end_ms,
            stage,
            query_results,
            aggregated=aggregated,
        )

    def _fetch_points_make_selects(self, metric_id, time_start_ms, time_end_ms, stage):
        # We fetch with ms precision, even though we only store with second
        # precision.
        row_size_ms_stage = _row_size_ms(stage)
        first_row = bg_utils.round_down(time_start_ms, row_size_ms_stage)
        last_row = bg_utils.round_down(time_end_ms, row_size_ms_stage)

        res = []
        # range(a,b) does not contain b, so we use last_row+1
        for row_start_ms in range(first_row, last_row + 1, row_size_ms_stage):
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
                statement, args = select
                res.append(_CassandraExecutionRequest(FETCH_POINT, statement, *args))

        return res

    def _update_metric_read_on(self, metric_name):
        if self.__read_on_sampling_rate == 0:
            return

        rate = int(1 / self.__read_on_sampling_rate)
        skip = self.__read_on_counter % rate > 0
        self.__read_on_counter += 1

        if skip:
            return

        log.debug("updating read_on for %s" % metric_name)
        self._execute_async_metadata(
            self.__update_metric_read_on_metadata_statement.with_params(metric_name)
        )

    def _select_metric(self, metric_name):
        """Fetch metric metadata."""
        encoded_metric_name = bg_metric.encode_metric_name(metric_name)
        result = list(
            self._execute_metadata(
                SELECT_METRIC_METADATA_LATENCY,
                self.__select_metric_metadata_statement.with_params(encoded_metric_name)
            )
        )
        if not result:
            return None

        # Check that id and config are non-null.
        result = result[0]
        if result[0] is None or result[1] is None:
            return None

        return result

    @tracing.trace
    def has_metric(self, metric_name):
        """See bg_accessor.Accessor."""
        super(_CassandraAccessor, self).has_metric(metric_name)
        metric_name = ".".join(self._components_from_name(metric_name)[:-1])
        metric_name = bg_metric.encode_metric_name(metric_name)
        tracing.add_attr_to_trace("metric.name", metric_name)
        if not self._select_metric(metric_name):
            return False

        # Small trick here: we also check that the parent directory
        # exists because that's what we check to create the directory
        # hierarchy.
        parent_dir = metric_name.rpartition(".")[0]
        if parent_dir and not self.has_directory(parent_dir):
            return False

        return True

    @tracing.trace
    def has_directory(self, directory):
        encoded_directory = bg_metric.encode_metric_name(directory)
        tracing.add_attr_to_trace("directory", encoded_directory)
        result = list(
            self._execute_metadata(
                HAS_DIRECTORY,
                _CassandraExecutionRequest(
                    SELECT_DIRECTORY,
                    self.__select_directory_statement,
                    encoded_directory
                )
            )
        )
        return bool(result)

    def __query_key(self, statement, params):
        if isinstance(statement, six.string_types):
            return statement
        else:
            if not params:
                return str(statement)
            else:
                return str(statement) + str(params)

    def _get_metrics(self, metric_names, strict_checks=False):
        metric_names = [".".join(self._components_from_name(metric_name)[:-1])
                        for metric_name in metric_names]
        metric_names = [bg_metric.encode_metric_name(metric_name) for metric_name in metric_names]

        execution_requests = [self._make_select_metric_execution_request(metric_name)
                              for metric_name in metric_names]

        results = self._select_metrics(execution_requests)

        metrics = []
        for (success, result) in results:
            if not success:
                raise CassandraError("Failed to concurrently get metrics", result)
            for row in result:
                metric = self._bind_metric(row, strict_checks=strict_checks)
                if metric is not None:
                    metrics.append(metric)
        return metrics

    def _make_select_metric_execution_request(self, metric_name):
        encoded_metric_name = bg_metric.encode_metric_name(metric_name)
        return self.__select_metric_metadata_statement.with_params(encoded_metric_name)

    def _select_metrics(self, execution_requests):
        return self._execute_concurrent_metadata(execution_requests)

    @tracing.trace
    def get_metric(self, metric_name, strict_checks=False):
        """See bg_accessor.Accessor."""
        super(_CassandraAccessor, self).get_metric(metric_name)
        tracing.add_attr_to_trace("metric.name", bg_metric.encode_metric_name(metric_name))
        return next(iter(self._get_metrics([metric_name], strict_checks=strict_checks)), None)

    def _bind_metric(self, row, strict_checks=False):
        uid = row[0]
        config = row[1]
        updated_on_raw = row[2]
        metric_name = row[3]

        # Return None if any of the important column is missing.
        if not uid or not config or not updated_on_raw:
            ERROR_BIND_METRIC.labels("column_missing").inc()
            log.warning(
                "Failed to bind metric; Missing column data: name:%s uid:%s config:%s updated_on:%s"
                % (metric_name, uid, config, updated_on_raw)
            )
            return None

        updated_on = datetime.datetime.fromtimestamp(updated_on_raw / 1000)

        # Used only when checking if metric exists; Do not try to get directories
        # when retrieving metrics for reading purpose.
        if strict_checks:
            parent_dir = metric_name.rpartition(".")[0]
            if parent_dir and not self.has_directory(parent_dir):
                return None

        try:
            metadata = bg_metric.MetricMetadata.from_string_dict(config)
        except Exception as e:
            ERROR_BIND_METRIC.labels("invalid_config").inc()
            log.warning(
                "Failed to bind metric; Invalid configuration: name:%s config:%s error:%s"
                % (metric_name, config, str(e))
            )
            return None

        return bg_metric.Metric(metric_name, uid, metadata, updated_on=updated_on)

    @tracing.trace
    def glob_directory_names(self, glob, start_time=None, end_time=None):
        """Return a sorted list of metric directories matching this glob."""
        super(_CassandraAccessor, self).glob_directory_names(glob)
        tracing.add_attr_to_trace("glob", glob)
        directory_names = self.__glob_names("directories", glob)
        return bg_glob.filter_from_glob(directory_names, glob)

    def glob_metrics(self, glob, start_time=None, end_time=None):
        """Return a sorted list of metrics matching this glob."""
        metric_names = self.glob_metric_names(glob, start_time, end_time)
        return self._get_metrics(metric_names)

    def glob_metric_names(self, glob, start_time=None, end_time=None):
        """Return a sorted list of metric names matching this glob."""
        super(_CassandraAccessor, self).glob_metric_names(glob)
        metric_names = self.__glob_names("metrics", glob)
        return bg_glob.filter_from_glob(metric_names, glob)

    def __glob_names(self, table, glob):
        if glob == "":
            return []

        components = self.__glob_parser.parse(glob)
        if len(components) > _COMPONENTS_MAX_LEN:
            raise bg_accessor.InvalidGlobError(
                "Contains %d components, but we support %d at most"
                % (len(components), _COMPONENTS_MAX_LEN)
            )

        counter = None
        if self.__glob_parser.is_fully_defined(components):
            # When the glob is a fully defined metric, we can take a shortcut.
            # `glob` can't be used directly because, for example a.{b} would be a.b.
            # We could probably extract some code from cassandra_sasi to do this when globs
            # are combinations of fully defined paths.
            if table == "metrics":
                query = self.__select_metric_statement
                counter = SELECT_METRIC
            else:
                query = self.__select_directory_statement
                counter = SELECT_DIRECTORY
            path = DIRECTORY_SEPARATOR.join([c[0] for c in components])
            queries = [query.bind([path])]
        else:
            queries = self.metadata_query_generator.generate_queries(table, components)
            counter = SELECT

        execution_requests = []
        for query in queries:
            if isinstance(query, six.string_types):
                execution_request = _CassandraExecutionRequest(
                    counter,
                    c_query.SimpleStatement(query, consistency_level=self._meta_read_consistency)
                )
            else:
                execution_request = _CassandraExecutionRequest(counter, query)
            execution_requests.append(execution_request)

        if self.cache:
            # As queries can be statements, we use the string representation
            # (which always contains the query and the parameters).
            # WARNING: With the current code PrepareStatement would not be cached.
            keys_to_queries = {
                self.__query_key(ec.statement, ec.params): ec for ec in execution_requests
            }
            cached_results = self.cache.get_many(keys_to_queries.keys())
            for key in cached_results:
                execution_requests.remove(keys_to_queries[key])
        else:
            cached_results = {}

        query_results = self._execute_concurrent_metadata(
            execution_requests,
            concurrency=self.max_concurrent_queries_per_pattern,
            results_generator=True,
            raise_on_first_error=True,
        )

        def _extract_results(query_results):
            n_metrics = 0
            fetched_results = collections.defaultdict(list)

            too_many_metrics = TooManyMetrics(
                "Query %s on %s yields more than %d results"
                % (glob, table, self.max_metrics_per_pattern)
            )

            for _, names in cached_results.items():
                for name in names:
                    n_metrics += 1
                    if n_metrics > self.max_metrics_per_pattern:
                        raise too_many_metrics
                    yield name

            try:
                for success, results in query_results:
                    key = self.__query_key(results.response_future.query, None)
                    # Make sure we also cache empty results.
                    fetched_results[key] = []
                    for result in results:
                        n_metrics += 1
                        name = result[0]
                        fetched_results[key].append(name)
                        if n_metrics > self.max_metrics_per_pattern:
                            raise too_many_metrics
                        yield name
            except cassandra.DriverException as e:
                raise CassandraError("Failed to glob: %s on %s" % (table, glob), e)

            if self.cache and fetched_results:
                self.cache.set_many(fetched_results, timeout=self.cache_metadata_ttl)

        return _extract_results(query_results)

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
        if self.__lazy_statements:
            self.__lazy_statements.flush()

    def insert_points_async(self, metric, datapoints, on_done=None):
        """See bg_accessor.Accessor."""
        super(_CassandraAccessor, self).insert_points_async(metric, datapoints, on_done)

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

        execution_requests = collections.defaultdict(list)
        for timestamp, value, count, stage in datapoints:
            timestamp_ms = int(timestamp) * 1000
            time_offset_ms = timestamp_ms % _row_size_ms(stage)
            time_start_ms = timestamp_ms - time_offset_ms
            offset = stage.step_ms(time_offset_ms)

            statement, args = self.__lazy_statements.prepare_insert(
                stage=stage,
                metric_id=metric.id,
                time_start_ms=time_start_ms,
                offset=offset,
                value=value,
                count=count,
            )

            # We group by table/partition.
            key = (stage, time_start_ms)
            execution_requests[key].append(
                _CassandraExecutionRequest(INSERT_DOWNSAMPLED_POINTS, statement, *args)
            )

        for execution_requests_for_partition in execution_requests.values():
            execution_request = None
            if len(execution_requests_for_partition) == 1:
                execution_request = execution_requests_for_partition[0]
                count = 1
            else:
                batch = c_query.BatchStatement(
                    consistency_level=self._data_write_consistency,
                    batch_type=c_query.BatchType.UNLOGGED,
                )
                for execution_request in execution_requests_for_partition:
                    if execution_request is not None:
                        batch.add(execution_request.statement, execution_request.params)
                        execution_request = _CassandraExecutionRequest(
                            INSERT_DOWNSAMPLED_POINTS_BATCH, batch
                        )
                count = len(execution_requests_for_partition)

            future = self._execute_async(
                session=self.__session_data, execution_request=execution_request
            )
            if count_down:
                if count > 1:
                    # If batched we will get less results.
                    count_down.count -= count - 1
                future.add_callbacks(
                    count_down.on_cassandra_result, count_down.on_cassandra_failure
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

    def syncdb(self, retentions=None, dry_run=False):
        """See bg_accessor.Accessor."""
        schema = ""

        was_connected = self.is_connected
        if not self.is_connected:
            self._connect_clusters()

        if self.metadata_enabled:
            self.__cluster_metadata.refresh_schema_metadata()
            keyspaces = self.__cluster_metadata.metadata.keyspaces.keys()
            if self.keyspace_metadata not in keyspaces:
                raise CassandraError("Missing keyspace '%s'." % self.keyspace_metadata)

            queries = _METADATA_CREATION_CQL
            if self.use_lucene:
                queries += _METADATA_CREATION_CQL_LUCENE
            else:
                queries += _METADATA_CREATION_CQL_SASI

            for cql in queries:
                query = cql % {"keyspace": self.keyspace_metadata}
                schema += query + "\n\n"
                if not dry_run:
                    self._execute_metadata(
                        SYNCDB_METADATA,
                        _CassandraExecutionRequest(METADATA_SCHEMA_UPDATE, query)
                    )

        self.__cluster_data.refresh_schema_metadata()
        keyspaces = self.__cluster_data.metadata.keyspaces.keys()
        if self.keyspace not in keyspaces:
            raise CassandraError("Missing keyspace '%s'." % self.keyspace)

        schema += " -- Already Existing Tables (updated schemas)\n"
        tables = self.__cluster_data.metadata.keyspaces[self.keyspace].tables
        for table in tables:
            comment = tables[table].options.get("comment")
            if table.startswith("datapoints_"):
                if not comment:
                    continue
                stage_str = json.loads(comment).get("stage")
                if not stage_str:
                    continue
                try:
                    stage = bg_metric.Stage.from_string(stage_str)
                except Exception as e:
                    log.debug(e)
                    continue
                query = self.__lazy_statements._create_datapoints_table_stmt(stage)
                schema += query + "\n\n"

        if retentions:
            schema += " -- New Tables\n"
            stages = set([s for retention in retentions for s in retention.stages])
            for stage in stages:
                query = self.__lazy_statements._create_datapoints_table_stmt(stage)
                schema += query + "\n\n"
                if not dry_run:
                    self._execute_data(
                        SYNCDB_DATA,
                        _CassandraExecutionRequest(SYNCDB, query)
                    )

        if not was_connected:
            self.shutdown()
        return schema

    def touch_metric(self, metric):
        """See the real Accessor for a description."""
        super(_CassandraAccessor, self).touch_metric(metric)

        if self.__bulkimport:
            return

        if not metric.updated_on:
            # updated_on is not set, it means the metric has
            # not been read yet from cassandra and we do nothing
            return
        else:
            delta = int(time.time()) - int(time.mktime(metric.updated_on.timetuple()))

        if delta >= self.__metadata_touch_ttl_sec:
            # Make sure the caller also see the change without refreshing
            # the metric.
            metric.updated_on = datetime.datetime.now()

            # Update Cassandra.
            self._execute_async_metadata(
                self.__touch_metrics_metadata_statement.with_params(metric.name)
            )

    def map(
        self,
        callback,
        start_key=None,
        end_key=None,
        shard=1,
        nshards=0,
        errback=None,
        callback_on_progress=None,
    ):
        """See bg_accessor.Accessor.

        Slight change for start_key and end_key, they are interpreted as
        tokens directly.
        """
        start_token, stop_token = self._get_search_range(
            start_key, end_key, shard, nshards
        )

        select = self._prepare_background_request(
            "SELECT name, token(name), id, config, "
            "dateOf(created_on), dateOf(updated_on), dateOf(read_on) "
            'FROM "%s".metrics_metadata '
            "WHERE token(name) > ? LIMIT %d ;"
            % (self.keyspace_metadata, DEFAULT_MAX_BATCH_UTIL)
        )
        select.request_timeout = None
        select.fetch_size = DEFAULT_MAX_BATCH_UTIL

        ignored_errors = 0
        token = start_token
        rows = []
        done = 0
        total = stop_token - start_token

        while token < stop_token:
            # Schedule read first.
            future = self._execute_async_metadata(
                _CassandraExecutionRequest(
                    MAP_ITERATION,
                    select,
                    int(token)
                ),
                timeout=DEFAULT_TIMEOUT_QUERY_UTIL
            )

            # Then execute callback for the *previous* result while C* is
            # doing its work.
            for i, result in enumerate(rows):
                metric_name = result[0]
                uid = result[2]
                config = result[3]
                done = token - start_token
                created_on, updated_on, read_on = result[4:]

                if not uid and not config:
                    log.debug("Skipping partial metric: %s" % metric_name)
                    if errback:
                        errback(metric_name)
                    continue

                try:
                    metadata = bg_metric.MetricMetadata.from_string_dict(config)
                    metric = bg_metric.Metric(
                        metric_name, uid, metadata, created_on, updated_on, read_on
                    )
                # Avoid failing if either name, id, or metadata is missing.
                except Exception as e:
                    log.debug(
                        "Skipping corrupted metric: %s raising %s" % (result, str(e))
                    )
                    if errback:
                        errback(metric_name)
                    continue

                callback(metric, done + 1, total)
                if callback_on_progress:
                    callback_on_progress(done + 1, total, token)

            # Then, read new data.
            try:
                rows = future.result()

                # Empty results means that we've reached the end.
                if len(rows.current_rows) == 0:
                    break
            except Exception as e:
                log.exception("Got exception: %s at token %s" % (e, token))
                # Put sleep a little bit to not stress Cassandra too mutch.
                time.sleep(1)
                token += DEFAULT_MAX_BATCH_UTIL
                ignored_errors += 1
                if ignored_errors > BATCH_MAX_IGNORED_ERRORS:
                    break
                continue

            token = rows[-1][1]

    def sync_map(
        self,
        callback,
        start_key=None,
        end_key=None,
        shard=1,
        nshards=0,
        errback=None,
        callback_on_progress=None,
    ):
        """See bg_accessor.Accessor.

        Slight change for start_key and end_key, they are interpreted as
        tokens directly.

        This functions is only used for experimental statistical analysis.
        """
        start_token, stop_token = self._get_search_range(
            start_key, end_key, shard, nshards
        )

        select = self._prepare_background_request(
            "SELECT name, token(name), id, config, "
            "dateOf(created_on), dateOf(updated_on), dateOf(read_on) "
            'FROM "%s".metrics_metadata '
            "WHERE token(name) > ? LIMIT %d ;"
            % (self.keyspace_metadata, DEFAULT_MAX_BATCH_UTIL)
        )
        select.request_timeout = None
        select.fetch_size = DEFAULT_MAX_BATCH_UTIL

        ignored_errors = 0
        token = start_token
        rows = []
        done = 0
        total = stop_token - start_token

        while token < stop_token:
            # Schedule read first.
            future = self._execute_async_metadata(
                _CassandraExecutionRequest(
                    MAP_ITERATION,
                    select,
                    int(token)
                ),
                timeout=DEFAULT_TIMEOUT_QUERY_UTIL
            )

            # Then, read new data.
            try:
                rows = future.result()

                # Empty results means that we've reached the end.
                if len(rows.current_rows) == 0:
                    break
            except Exception as e:
                log.exception("Got exception: %s at token %s" % (e, token))
                # Put sleep a little bit to not stress Cassandra too mutch.
                time.sleep(1)
                token += DEFAULT_MAX_BATCH_UTIL
                ignored_errors += 1
                if ignored_errors > BATCH_MAX_IGNORED_ERRORS:
                    break
                continue

            returned_rows = len(rows.current_rows)

            # Then execute callback for the *previous* result while C* is
            # doing its work.
            for i, result in enumerate(rows):
                metric_name = result[0]
                token = result[1]
                uid = result[2]
                config = result[3]
                done = token - start_token
                created_on, updated_on, read_on = result[4:]

                # Stop if token > stop_token
                if token >= stop_token:
                    break

                if not uid and not config:
                    log.debug("Skipping partial metric: %s" % metric_name)
                    if errback:
                        errback(metric_name)
                    continue

                try:
                    metadata = bg_metric.MetricMetadata.from_string_dict(config)
                    metric = bg_metric.Metric(
                        metric_name, uid, metadata, created_on, updated_on, read_on
                    )
                # Avoid failing if either name, id, or metadata is missing.
                except Exception as e:
                    log.debug(
                        "Skipping corrupted metric: %s raising %s" % (result, str(e))
                    )
                    if errback:
                        errback(metric_name)
                    continue

                callback(metric, done + 1, total)
                if callback_on_progress:
                    callback_on_progress(done + 1, total, token)

            if returned_rows == 0:
                break

    def map_empty_directories_sync(
        self,
        callback,
        start_key=None,
        end_key=None,
        shard=1,
        nshards=0,
        errback=None,
        callback_on_progress=None,
        num_token_ignore_on_error=DEFAULT_MAX_BATCH_UTIL,
    ):
        """Scan for directory that does not contains any metrics.

        This functions is only used for experimental statistical analysis.
        It should be merged with the cleaning method to remove deduplicated code.
        """
        start_token, stop_token = self._get_search_range(
            start_key, end_key, shard, nshards
        )

        dir_query = self._prepare_background_request(
            "SELECT name, token(name)"
            ' FROM "%s".directories'
            " WHERE token(name) > ? LIMIT %d;"
            % (self.keyspace_metadata, DEFAULT_MAX_BATCH_UTIL)
        )
        has_metric_query = self._prepare_background_request(
            'SELECT name FROM "%s".metrics'
            " WHERE parent LIKE ? LIMIT 1;" % (self.keyspace_metadata,)
        )

        def directories_to_check(result, stop_token):
            for row in result:
                name, next_token = row
                if name and next_token < stop_token:
                    yield _CassandraExecutionRequest(
                        CLEAN_DIRECTORIES_TO_CHECK,
                        has_metric_query,
                        name + DIRECTORY_SEPARATOR + "%"
                    )

        # Reset the error count.
        ignored_errors = 0

        log.info("Starting cleanup of empty dir")
        token = start_token
        while token < stop_token:
            CLEAN_CURRENT_OFFSET.set(token)
            try:
                result = self._execute_metadata(
                    CLEAN_EMPTY_DIR,
                    _CassandraExecutionRequest(
                        CLEAN,
                        dir_query,
                        int(token)
                    ),
                    timeout=DEFAULT_TIMEOUT_QUERY_UTIL
                )

                # Reset the error count.
                ignored_errors = 0

                # End was reached
                if len(result.current_rows) == 0:
                    break
            except Exception as e:
                log.exception("Got exception: %s at token %s" % (e, token))
                # Put sleep a little bit to not stress Cassandra too mutch.
                time.sleep(1)
                ignored_errors += 1

                # After a few retries on the same query, lets move on.
                if ignored_errors % 3 == 0:
                    token += num_token_ignore_on_error
                    CLEAN_SKIPPED_OFFSET.inc(num_token_ignore_on_error)

                # If we failed too much, let's just stop the process.
                if ignored_errors > BATCH_MAX_IGNORED_ERRORS:
                    break
                continue

            # Update token range for the next iteration
            token = result[-1][1]

            parent_dirs = self._execute_concurrent_metadata(
                directories_to_check(result, stop_token),
                concurrency=self.max_concurrent_connections,
                raise_on_first_error=False,
            )

            for response in parent_dirs:
                if not response.success:
                    print("failed to query for metrics in directory")
                    continue

                dir_name = response.result_or_exc.response_future.query.values[0]
                dir_name = str(dir_name.decode('ascii'))
                dir_name = str(dir_name).rpartition(".")[0]

                callback(dir_name, list(response.result_or_exc))

            if callback_on_progress:
                callback_on_progress(token - start_token, stop_token - start_token, token)

    def repair(
        self,
        start_key=None,
        end_key=None,
        shard=0,
        nshards=1,
        callback_on_progress=None,
    ):
        """See bg_accessor.Accessor.

        Slight change for start_key and end_key, they are intrepreted as
        tokens directly.
        """
        super(_CassandraAccessor, self).repair(start_key, end_key, shard, nshards)

        disable_repair = os.getenv('DISABLE_REPAIR_MISSING_DIR')
        if disable_repair is None:
            self._repair_missing_dir(
                start_key, end_key, shard, nshards, callback_on_progress
            )

        disable_invalid_metrics = os.getenv('DISABLE_REPAIR_INVALID_METRICS')
        if disable_invalid_metrics is None:
            self._delete_invalid_metrics(
                start_key, end_key, shard, nshards, callback_on_progress
            )

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

        return start_token, stop_token

    def _delete_invalid_metrics(
        self,
        start_key=None,
        end_key=None,
        shard=0,
        nshards=1,
        callback_on_progress=None,
    ):
        """Iterate metrics records & delete those without metrics_metadata record."""
        start_token, stop_token = self._get_search_range(
            start_key, end_key, shard, nshards
        )

        metrics_query = self._prepare_background_request(
            'SELECT name, token(name) FROM "%s".metrics '
            "WHERE token(name) > ? LIMIT %d;"
            % (self.keyspace_metadata, DEFAULT_MAX_BATCH_UTIL)
        )

        check_query = self._prepare_background_request(
            'SELECT name, token(name) FROM "%s".metrics_metadata '
            'WHERE name = ?;'
            % (self.keyspace_metadata)
        )

        delete_query = self._prepare_background_request(
            'DELETE FROM "%s".metrics WHERE name = ?;'
            % (self.keyspace_metadata)
        )

        ignored_errors = 0
        token = start_token
        while token < stop_token:
            try:
                result = self._execute_metadata(
                    CLEAN_BROKEN_METRICS,
                    _CassandraExecutionRequest(
                        CLEAN_BROKEN_METRICS_COUNT,
                        metrics_query,
                        int(token)
                    ),
                    timeout=DEFAULT_TIMEOUT_QUERY_UTIL
                )

                # Reset the error count.
                ignored_errors = 0

                # Empty results means that we've reached the end.
                if len(result.current_rows) == 0:
                    break
            except Exception as e:
                log.exception("Got exception: %s at token %s" % (e, token))
                # Put sleep a little bit to not stress Cassandra too mutch.
                time.sleep(1)
                ignored_errors += 1

                if ignored_errors > BATCH_MAX_IGNORED_ERRORS:
                    break

                continue

            def metrics_to_check(rows):
                for row in result:
                    name, next_token = row
                    yield _CassandraExecutionRequest(
                        BROKEN_METRICS_TO_CHECK,
                        check_query,
                        name
                    )

            token = result[-1][1]
            metrics_checked = self._execute_concurrent_metadata(
                metrics_to_check(result),
                concurrency=self.max_concurrent_connections,
                raise_on_first_error=False,
            )

            def metrics_to_remove(result):
                for response in result:
                    if not response.success:
                        continue

                    results = list(response.result_or_exc)
                    if results:
                        continue

                    name = response.result_or_exc.response_future.query.values[0]
                    name = str(name.decode('ascii'))

                    log.info("Scheduling delete for metric '%s'" % name)
                    yield _CassandraExecutionRequest(
                        BROKEN_METRICS_TO_REMOVE,
                        delete_query,
                        name
                    )

            rets = self._execute_concurrent_metadata(
                metrics_to_remove(metrics_checked),
                concurrency=self.max_concurrent_connections,
                raise_on_first_error=False,
            )

            for ret in rets:
                if not ret.success:
                    print(str(ret.result_or_exc))
                    log.warning(str(ret.result_or_exc))

            if callback_on_progress:
                callback_on_progress(token - start_token, stop_token - start_token, token)

    def _repair_missing_dir(
        self,
        start_key=None,
        end_key=None,
        shard=0,
        nshards=1,
        callback_on_progress=None,
    ):
        """Create directory that does not exist for a metric to be accessible."""
        start_token, stop_token = self._get_search_range(
            start_key, end_key, shard, nshards
        )

        dir_query = self._prepare_background_request(
            'SELECT name, token(name) FROM "%s".directories'
            " WHERE token(name) > ? LIMIT %d;"
            % (self.keyspace_metadata, DEFAULT_MAX_BATCH_UTIL)
        )

        has_directory_query = self._prepare_background_request(
            'SELECT name FROM "%s".directories'
            " WHERE name = ? LIMIT 1;" % self.keyspace_metadata
        )

        def directories_to_check(result):
            for row in result:
                name, next_token = row
                parent_dir = name.rpartition(".")[0]
                if parent_dir:
                    yield _CassandraExecutionRequest(
                        REPAIR_DIRECTORIES_TO_CHECK,
                        has_directory_query,
                        parent_dir
                    )

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
                components = self._components_from_name(
                    dir_name + DIRECTORY_SEPARATOR + "_"
                )
                queries = self._create_parent_dirs_queries(components)
                for query in queries:
                    PM_REPAIRED_DIRECTORIES.inc()
                    yield _CassandraExecutionRequest(
                        REPAIR_DIRECTORIES_TO_CREATE,
                        query
                    )

        log.info("Start creating missing directories")
        token = start_token
        while token < stop_token:
            result = self._execute_metadata(
                REPAIR_MISSING_DIR,
                _CassandraExecutionRequest(
                    REPAIR_MISSING_DIR_COUNT,
                    dir_query,
                    int(token)
                ),
                timeout=DEFAULT_TIMEOUT_QUERY_UTIL
            )
            if len(result.current_rows) == 0:
                break

            # Update token range for the next iteration
            token = result[-1][1]
            parent_dirs = self._execute_concurrent_metadata(
                directories_to_check(result),
                concurrency=self.max_concurrent_connections,
                raise_on_first_error=False,
            )

            rets = self._execute_concurrent_metadata(
                directories_to_create(parent_dirs),
                concurrency=self.max_concurrent_connections,
                raise_on_first_error=False,
            )

            for ret in rets:
                if not ret.success:
                    log.warning(str(ret.result_or_exc))

            if callback_on_progress:
                callback_on_progress(token - start_token, stop_token - start_token, token)

    def _clean_empty_dir(
        self,
        start_key=None,
        end_key=None,
        shard=0,
        nshards=1,
        callback_on_progress=None,
        num_token_ignore_on_error=DEFAULT_MAX_BATCH_UTIL,
    ):
        """Remove directory that does not contains any metrics."""
        start_token, stop_token = self._get_search_range(
            start_key, end_key, shard, nshards
        )

        dir_query = self._prepare_background_request(
            "SELECT name, token(name)"
            ' FROM "%s".directories'
            " WHERE token(name) > ? LIMIT %d;"
            % (self.keyspace_metadata, DEFAULT_MAX_BATCH_UTIL)
        )
        has_metric_query = self._prepare_background_request(
            'SELECT name FROM "%s".metrics'
            " WHERE parent LIKE ? LIMIT 1;" % (self.keyspace_metadata,)
        )
        delete_empty_dir_stm = self._prepare_background_request(
            'DELETE FROM "%s".directories' " WHERE name = ?;" % self.keyspace_metadata
        )

        def directories_to_check(result):
            for row in result:
                name, next_token = row
                if name:
                    yield _CassandraExecutionRequest(
                        CLEAN_DIRECTORIES_TO_CHECK,
                        has_metric_query,
                        name + DIRECTORY_SEPARATOR + "%"
                    )

        def directories_to_remove(result):
            for response in result:
                if not response.success:
                    log.warning(str(response.result_or_exc))
                    continue
                results = list(response.result_or_exc)
                if results:
                    continue
                dir_name = response.result_or_exc.response_future.query.values[0]
                dir_name = str(dir_name.decode('ascii'))
                dir_name = str(dir_name).rpartition(".")[0]
                log.info("Scheduling delete for empty dir '%s'" % dir_name)
                PM_DELETED_DIRECTORIES.inc()
                yield _CassandraExecutionRequest(
                    CLEAN_DIRECTORIES_TO_REMOVE,
                    delete_empty_dir_stm,
                    dir_name
                )

        # Reset the error count.
        ignored_errors = 0

        log.info("Starting cleanup of empty dir")
        token = start_token
        while token < stop_token:
            CLEAN_CURRENT_OFFSET.set(token)
            try:
                result = self._execute_metadata(
                    CLEAN_EMPTY_DIR,
                    _CassandraExecutionRequest(
                        CLEAN,
                        dir_query,
                        int(token)
                    ),
                    timeout=DEFAULT_TIMEOUT_QUERY_UTIL
                )

                # Reset the error count.
                ignored_errors = 0

                # End was reached
                if len(result.current_rows) == 0:
                    break
            except Exception as e:
                log.exception("Got exception: %s at token %s" % (e, token))
                # Put sleep a little bit to not stress Cassandra too mutch.
                time.sleep(1)
                ignored_errors += 1

                # After a few retries on the same query, lets move on.
                if ignored_errors % 3 == 0:
                    token += num_token_ignore_on_error
                    CLEAN_SKIPPED_OFFSET.inc(num_token_ignore_on_error)

                # If we failed too much, let's just stop the process.
                if ignored_errors > BATCH_MAX_IGNORED_ERRORS:
                    break
                continue

            # Update token range for the next iteration
            token = result[-1][1]
            parent_dirs = self._execute_concurrent_metadata(
                directories_to_check(result),
                concurrency=self.max_concurrent_connections,
                raise_on_first_error=False,
            )
            rets = self._execute_concurrent_metadata(
                directories_to_remove(parent_dirs),
                concurrency=self.max_concurrent_connections,
                raise_on_first_error=False,
            )
            for ret in rets:
                if not ret.success:
                    log.warning(str(ret.result_or_exc))

            if callback_on_progress:
                callback_on_progress(token - start_token, stop_token - start_token, token)

    def clean(
        self,
        max_age=None,
        start_key=None,
        end_key=None,
        shard=1,
        nshards=0,
        callback_on_progress=None,
        disable_clean_directories=False,
        disable_clean_metrics=False,
    ):
        """See bg_accessor.Accessor.

        Args:
            cutoff: UNIX time in seconds. Rows older than it should be deleted.
            max_age: int, Max metric age for expiration
            start_key: int, Start token to do cleaning (int64)
            end_key: int, End token to do cleaning (int64)
            shard: int, Shard to clean
            nshards: int, Total number of shards
            disable_clean_directories: bool, Disable directories cleaning
            disable_clean_metrics: bool, Disable metrics cleaning
            callback_on_progress: function to track operations progress
        """
        super(_CassandraAccessor, self).clean(max_age, callback_on_progress)

        CLEAN_START_TS_METRIC.set(time.time())
        CLEAN_STOP_TS_METRIC.set(0)
        CLEAN_CURRENT_STEP.set(0)

        first_exception = None

        # 0.006% of database
        num_token_ignore_on_error = 2 ** 50

        env_num_token = environ.get('CLEAN_IGNORE_BATCH_SIZE_ON_ERROR')
        if env_num_token is not None and env_num_token.isdigit():
            exponent = int(env_num_token)
            if exponent < 16 or exponent > 62:
                log.exception("Invalid CLEAN_IGNORE_BATCH_SIZE_ON_ERROR given: ignored")
            else:
                num_token_ignore_on_error = 2 ** exponent

        # First, clean metrics...
        if not disable_clean_metrics:
            CLEAN_CURRENT_STEP.set(1)
            ts_start = time.time()
            try:
                self._clean_expired_metrics(
                    max_age,
                    start_key,
                    end_key,
                    shard,
                    nshards,
                    callback_on_progress,
                    num_token_ignore_on_error,
                )
            except Exception as e:
                first_exception = e
                log.exception("Failed to clean metrics.")
            CLEAN_CURRENT_STEP.set(2)
            CLEAN_STEP_TS.labels('expired_metrics').set(time.time() - ts_start)
        else:
            log.info("Cleaning metrics was disabled")

        # Then, clean directories...
        if not disable_clean_directories:
            CLEAN_CURRENT_STEP.set(3)
            ts_start = time.time()
            try:
                self._clean_empty_dir(
                    start_key,
                    end_key,
                    shard,
                    nshards,
                    callback_on_progress,
                    num_token_ignore_on_error,
                )
            except Exception as e:
                first_exception = e
                log.exception("Failed to clean directories.")
            CLEAN_CURRENT_STEP.set(4)
            CLEAN_STEP_TS.labels('empty_directories').set(time.time() - ts_start)
        else:
            log.info("Cleaning directories was disabled")

        CLEAN_STOP_TS_METRIC.set(time.time())

        if first_exception is not None:
            raise_with_traceback(first_exception)

    def _prepare_background_request(
        self,
        query_str,
        timeout=DEFAULT_TIMEOUT_QUERY_UTIL,
        consistency=None,
    ):
        select = self.__session_metadata.prepare(query_str)
        if consistency is not None:
            select.consistency_level = consistency
        else:
            select.consistency_level = self._meta_background_consistency

        # Always retry background requests a few times, we don't really care
        # about latency.
        select.retry_policy = bg_cassandra_policies.AlwaysRetryPolicy()
        select.request_timeout = timeout

        return select

    def _clean_expired_metrics(
        self,
        max_age=None,
        start_key=None,
        end_key=None,
        shard=1,
        nshards=0,
        callback_on_progress=None,
        num_token_ignore_on_error=DEFAULT_MAX_BATCH_UTIL,
    ):
        """Delete metrics that has an expired ttl."""
        if not max_age:
            log.warn("You must specify a cutoff time for cleanup")
            return

        start_token, stop_token = self._get_search_range(
            start_key, end_key, shard, nshards
        )

        # timestamp format in Cassandra is in milliseconds
        cutoff = (int(time.time()) - max_age) * 1000
        log.info("Cleaning with cutoff time %d", cutoff)

        # select method: traditional, unfiltered, adaptative
        available_methods = ['traditional', 'unfiltered', 'adaptative']
        method = 'traditional'

        # use env var to select method.
        env_method = environ.get('CLEAN_OUTDATED_METHOD')
        if env_method is not None and env_method in available_methods:
            method = env_method

        env_batch_size = environ.get('CLEAN_BATCH_SIZE')
        if env_batch_size is not None and env_batch_size.isdigit():
            batch_sizes = [int(env_batch_size)]
        else:
            batch_sizes = [1000, 500, 250, 125, 75, 35]
        select_queries = []
        current_select_index = 0

        # Metrics dump is only compatible with METHOD='unfiltered'
        metrics_dump_file = environ.get('CLEAN_DUMP_METRICS', None)
        metrics_dump_file_fd = None

        if metrics_dump_file is not None:
            if env_method != 'unfiltered':
                log.warning('Can not dump all metrics when not using "unfiltered" method')
            else:
                log.info(f'Opening {metrics_dump_file} to dump all metrics.')
                # Using gzip for small files.
                metrics_dump_file_fd = gzip.open(metrics_dump_file, 'wt')

        # statements
        for batch_size in batch_sizes:
            if method in ['traditional', 'adaptative']:
                # Enforcing consistency one on this one, as it will eventually fail if not.
                select = _CassandraExecutionRequest(
                    CLEAN_EXPIRED_METRICS_SELECT,
                    self._prepare_background_request(
                        'SELECT name, token(name), toUnixTimestamp(updated_on) '
                        'FROM "%s".metrics_metadata '
                        'WHERE updated_on <= maxTimeuuid(%d) and token(name) > ? LIMIT %d;'
                        % (self.keyspace_metadata, cutoff, batch_size),
                        consistency=cassandra.ConsistencyLevel.ONE,
                        timeout=None,
                    )
                )
            else:
                select = _CassandraExecutionRequest(
                    CLEAN_EXPIRED_METRICS_SELECT,
                    self._prepare_background_request(
                        'SELECT name, token(name), toUnixTimestamp(updated_on) '
                        'FROM "%s".metrics_metadata '
                        'WHERE token(name) > ? LIMIT %d;'
                        % (self.keyspace_metadata, DEFAULT_MAX_BATCH_UTIL),
                        timeout=None,
                    )
                )

            select_queries.append(select)

        delete = _CassandraExecutionRequest(
            CLEAN_EXPIRED_METRICS_DELETE,
            self._prepare_background_request(
                'DELETE FROM "%s".metrics WHERE name = ? ;' % (self.keyspace_metadata)
            )
        )
        delete_metadata = _CassandraExecutionRequest(
            CLEAN_EXPIRED_METRICS_DELETE_METADATA,
            self._prepare_background_request(
                'DELETE FROM "%s".metrics_metadata WHERE name = ? ;'
                % (self.keyspace_metadata)
            )
        )

        def run(method, cutoff, rows, metrics_dump_file_fd=None):
            for name, _, updated_on in rows:
                if method == 'unfiltered':
                    to_delete = True
                    if updated_on is None:
                        log.info("Skipping metric %s as no updated_on date was present.", name)
                        to_delete = False

                    if to_delete is True and updated_on > cutoff:
                        log.info("Skipping delete for non-obsolete metric %s", name)
                        to_delete = False

                    if to_delete is False:
                        if metrics_dump_file_fd is not None:
                            metrics_dump_file_fd.write(f"{name}\n")
                            pass

                        continue

                log.info("Scheduling delete for obsolete metric %s", name)
                PM_EXPIRED_METRICS.inc()
                yield (delete.with_params(name))
                yield (delete_metadata.with_params(name))

        ignored_errors = 0
        successful_queries = 0
        token = start_token
        while token < stop_token:
            CLEAN_CURRENT_OFFSET.set(token)
            try:
                rows = self._execute_metadata(
                    CLEAN_EXPIRED_METRICS,
                    select_queries[current_select_index].with_params(int(token)),
                    timeout=DEFAULT_TIMEOUT_QUERY_UTIL
                )

                # Reset the error count.
                ignored_errors = 0

                # Empty results means that we've reached the end.
                if len(rows.current_rows) == 0:
                    break
            except Exception as e:
                log.exception("Got exception: %s at token %s" % (e, token))
                # Put sleep a little bit to not stress Cassandra too mutch.
                time.sleep(1)
                ignored_errors += 1
                successful_queries = 0

                # After a few retries on the same query, lets move on.
                if ignored_errors % 3 == 0:
                    if method == 'adaptative' and current_select_index != len(select_queries) - 1:
                        # Too much errors with this batch size: limiting number of results.
                        current_select_index += 1
                    else:
                        token += num_token_ignore_on_error
                        CLEAN_SKIPPED_OFFSET.inc(num_token_ignore_on_error)

                # If we failed too much, let's just stop the process.
                if ignored_errors > BATCH_MAX_IGNORED_ERRORS:
                    break
                continue

            successful_queries += 1

            if successful_queries > 10 and current_select_index != 0:
                # Multiple queries worked successfully & we are not at the maximum batch size
                current_select_index -= 1
                successful_queries = 0

            token = rows[-1][1]
            rets = self._execute_concurrent_metadata(
                run(method, cutoff, rows, metrics_dump_file_fd),
                concurrency=self.max_concurrent_connections,
                raise_on_first_error=False,
            )

            for ret in rets:
                if not ret.success:
                    log.warn(str(ret.result_or_exc))

            if callback_on_progress:
                done = token - start_token
                total = stop_token - start_token
                callback_on_progress(done, total, token)

        # End of the process
        if metrics_dump_file_fd is not None:
            log.info(f'Closing {metrics_dump_file} and process is ending.')
            metrics_dump_file_fd.close()

    def metadata_enabled(self):
        """See bg_accessor.Accessor."""
        return self.metadata_enabled


def build(*args, **kwargs):
    """Return a bg_accessor.Accessor using Casssandra.

    Args:
      keyspace: Base name of Cassandra keyspaces dedicated to BigGraphite.
      contact_points: list of strings, the hostnames or IP to use to discover Cassandra.
      port: The port to connect to, as an int.
    """
    return _CassandraAccessor(*args, **kwargs)
