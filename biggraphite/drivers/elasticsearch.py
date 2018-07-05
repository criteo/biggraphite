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
"""Simple ElasticSearch-based accessor for tests and development."""
from __future__ import absolute_import
from __future__ import print_function

import collections
import datetime
import uuid
import logging
import elasticsearch
# import elasticsearch_dsl

import sortedcontainers

from biggraphite import accessor as bg_accessor
from biggraphite import glob_utils as bg_glob
from biggraphite.drivers import _downsampling
from biggraphite.drivers import _utils
from biggraphite.drivers import _delayed_writer

log = logging.getLogger(__name__)

# TODO:
# * Support metadata
#   * Metrics
#   * Directories
# * Add unit tests (with real ES)
# * Support data
# * Support dated indices
# * Handle timeouts, error
# * Implement repair
# * Implement clean

# TODO: Make that configurable (in a file), this will be particularly important
# for the number of shards and replicas.
INDEX_SETTINGS = {
    "settings": {
        "index": {
            "number_of_shards": 3,
            "number_of_replicas": 1,
            "refresh_interval": 60,
            "translog": {
                "sync_interval": 120,
                "durability": "async",
            },
            "search": {
                "slowlog": {
                    "level": "info",
                    "threshold": {
                        "query": {
                            "debug": "2s",
                            "info": "5s",
                        },
                        "fetch": {
                            "debug": "200ms",
                            "info": "500ms",
                        },
                    }
                }
            }
        },
    },
    "mappings": {
        "_doc": {
            "properties": {
                "depth": {"type": "long"},
                "created_on": {"type": "date"},
                "read_on": {"type": "date"},
                "updated_on": {"type": "date"},
                "name": {
                    "type": "keyword",
                    "ignore_above": 1024,
                },
                "uuid": {
                    "type": "keyword",
                },
                "config": {
                    "type": "object",
                    # TODO: describe existing fields with more details.
                },
            },
            # Additional properties (such as path components) or labels
            # TODO: have a specific dynamic mapping for labels using "match"
            # FIXME: this doesn't seem to work, maybe because of text <-> string.
            "dynamic_templates": [
                {
                    "text_as_keywords": {
                        "match_mapping_type": "text",
                        "mapping": {
                            "type": "keyword",
                            "ignore_above": 256,
                            "ignore_malformed": True,
                        }
                    }
                }
            ]
        },
    },
}

DEFAULT_INDEX = "biggraphite_metrics"
DEFAULT_INDEX_SUFFIX = "_%Y-%m-%d"
INDEX_DOC_TYPE = "_doc"
DEFAULT_HOSTS = ["127.0.0.1"]
DEFAULT_PORT = 9200
DEFAULT_TIMEOUT = 10


OPTIONS = {
    "username": str,
    "password": str,
    "keyspace": str,
    "hosts": _utils.list_from_str,
    "port": int,
    "timeout": float,
}


def add_argparse_arguments(parser):
    """Add ElasticSearch arguments to an argparse parser."""
    parser.add_argument(
        "--elasticsearch_index",
        metavar="NAME",
        help="elasticsearch index.",
        default=DEFAULT_INDEX,
    )
    parser.add_argument(
        "--elasticsearch_index_suffix",
        metavar="NAME",
        help="elasticsearch index suffix. Supports strftime format.",
        default=DEFAULT_INDEX_SUFFIX,
    )
    parser.add_argument(
        "--elasticsearch_username", help="elasticsearch username.", default=None
    )
    parser.add_argument(
        "--elasticsearch_password", help="elasticsearch password.", default=None
    )
    parser.add_argument(
        "--elasticsearch_hosts",
        metavar="HOST[,HOST,...]",
        help="Hosts used for discovery.",
        default=DEFAULT_HOSTS,
    )
    parser.add_argument(
        "--elasticsearch_port",
        metavar="PORT",
        type=int,
        help="The native port to connect to.",
        default=DEFAULT_PORT,
    )
    parser.add_argument(
        "--elasticsearch_timeout",
        metavar="TIMEOUT",
        type=int,
        help="elasticsearch query timeout in seconds.",
        default=DEFAULT_TIMEOUT,
    )


def _components_from_name(metric_name):
    res = metric_name.split(".")
    return list(filter(None, res))


def document_from_metric(metric):
    """Creates an ElasticSearch document from a Metric."""
    config = metric.metadata.as_string_dict()
    components = _components_from_name(metric.name)
    name = ".".join(components)  # We do that to avoid double dots.

    data = {
        "depth": len(components) - 1,
        "name": name,
    }

    for i, component in enumerate(components):
        data["p%d" % i] = component
    data.update({
        "uuid": metric.id,
        "created_on": datetime.datetime.now(),
        "updated_on": datetime.datetime.now(),
        "read_on": None,
        "config": config,
    })
    return data


class Error(bg_accessor.Error):
    """Base class for all exceptions from this module."""


class InvalidArgumentError(Error, bg_accessor.InvalidArgumentError):
    """Callee did not follow requirements on the arguments."""


class _ElasticSearchAccessor(bg_accessor.Accessor):
    """A ElasticSearch acessor that doubles as a ElasticSearch MetadataCache."""

    Row = collections.namedtuple(
        "Row", ["time_start_ms", "offset", "shard", "value", "count"]
    )

    Row0 = collections.namedtuple("Row", ["time_start_ms", "offset", "value"])

    _UUID_NAMESPACE = uuid.UUID("{00000000-1111-2222-3333-444444444444}")

    def __init__(
        self,
        hosts=DEFAULT_HOSTS,
        port=DEFAULT_PORT,
        index=DEFAULT_INDEX,
        index_suffix=DEFAULT_INDEX_SUFFIX,
        username=None,
        password=None,
        timeout=DEFAULT_TIMEOUT,
    ):
        """Create a new ElasticSearchAccessor."""
        super(_ElasticSearchAccessor, self).__init__("ElasticSearch")
        self._metric_to_points = collections.defaultdict(sortedcontainers.SortedDict)
        self._name_to_metric = {}
        self._directory_names = sortedcontainers.SortedSet()
        self.__downsampler = _downsampling.Downsampler()
        self.__delayed_writer = _delayed_writer.DelayedWriter(self)
        self._hosts = list(hosts)
        self._port = port
        self._index_prefix = index
        self._index_suffix = index_suffix
        self._username = username
        self._password = password
        self._timeout = timeout
        self._known_indices = {}
        self.client = None

    @property
    def _metric_names(self):
        return self._name_to_metric.keys()

    def connect(self, *args, **kwargs):
        """See the real Accessor for a description."""
        super(_ElasticSearchAccessor, self).connect(*args, **kwargs)
        self._connect()
        self.is_connected = True

    def _connect(self):
        """Connect to elasticsearch."""
        if self.is_connected:
            return

        if self._username:
            http_auth = (self._username, self._password or "")
        else:
            http_auth = None
        es = elasticsearch.Elasticsearch(
            self._hosts,
            port=self._port,
            http_auth=http_auth,
            # FIXME: set to true
            sniff_on_start=False,
            sniff_on_connection_fail=False,
            retry_on_timeout=True,
            max_retries=3,
            timeout=self._timeout,
        )

        log.info("Connected: %s" % es.info())
        self.client = es

    def shutdown(self, *args, **kwargs):
        """See the real Accessor for a description."""
        super(_ElasticSearchAccessor, self).shutdown(*args, **kwargs)
        self._shutdown()
        self.is_connected = False

    def _shutdown(self):
        """Shutdown Elasticsearch client."""
        if self.client:
            self.client.transport.close()
            self.client = None

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
        if self.client:
            self.client.indices.flush(
                index="%s*" % self._index_prefix,
                allow_no_indices=True,
                ignore_unavailable=True,
                wait_if_ongoing=True,
            )

    def get_index(self, metric):
        """Get the index where a metric should be stored."""
        # Here the index could be sharded further by looking at the
        # metric metadata, for example, per owner.
        index_name = self._index_prefix + datetime.datetime.now().strftime(self._index_suffix)
        if index_name not in self._known_indices:
            self.client.indices.create(
                index=index_name,
                body=INDEX_SETTINGS,
                ignore=400
            )
            self._known_indices[index_name] = True
        return index_name

    def insert_points_async(self, metric, datapoints, on_done=None):
        """See the real Accessor for a description."""
        super(_ElasticSearchAccessor, self).insert_points_async(
            metric, datapoints, on_done
        )
        if metric.name not in self._name_to_metric:
            self.create_metric(metric)

        datapoints = self.__downsampler.feed(metric, datapoints)
        if self.__delayed_writer:
            datapoints = self.__delayed_writer.feed(metric, datapoints)
        return self.insert_downsampled_points_async(metric, datapoints, on_done)

    def insert_downsampled_points_async(self, metric, datapoints, on_done=None):
        """See the real Accessor for a description."""
        super(_ElasticSearchAccessor, self).insert_downsampled_points_async(
            metric, datapoints, on_done
        )
        if metric.name not in self._name_to_metric:
            self.create_metric(metric)

        for datapoint in datapoints:
            timestamp, value, count, stage = datapoint
            points = self._metric_to_points[(metric.name, stage)]
            points[timestamp] = (value, count)
        if on_done:
            on_done(None)

    def drop_all_metrics(self, *args, **kwargs):
        """See the real Accessor for a description."""
        super(_ElasticSearchAccessor, self).drop_all_metrics(*args, **kwargs)
        # Drop indices.
        self._metric_to_points.clear()
        self._name_to_metric.clear()
        self._directory_names.clear()

    def make_metric(self, name, metadata):
        """See bg_accessor.Accessor."""
        # Cleanup name (avoid double dots)
        name = ".".join(_components_from_name(name))
        uid = uuid.uuid5(self._UUID_NAMESPACE, name)
        now = datetime.datetime.now()
        return bg_accessor.Metric(
            name, uid, metadata, created_on=now, updated_on=now, read_on=None
        )

    def create_metric(self, metric):
        """See the real Accessor for a description."""
        super(_ElasticSearchAccessor, self).create_metric(metric)
        index_name = self.get_index(metric)
        self.client.create(
            index=index_name,
            doc_type=INDEX_DOC_TYPE,
            id=metric.id,
            body=document_from_metric(metric),
            ignore=409,
        )

    def update_metric(self, name, updated_metadata):
        """See bg_accessor.Accessor."""
        super(_ElasticSearchAccessor, self).update_metric(name, updated_metadata)
        # Cleanup name (avoid double dots)
        name = ".".join(_components_from_name(name))
        metric = self._name_to_metric[name]
        if not metric:
            raise InvalidArgumentError("Unknown metric '%s'" % name)
        metric.metadata = updated_metadata
        self._name_to_metric[name] = metric

    def delete_metric(self, name):
        del self._name_to_metric[name]

    def delete_directory(self, name):
        self._directory_names.remove(name)

    @staticmethod
    def __glob_names(names, glob):
        results = bg_glob.glob(names, glob)
        results.sort()
        return results

    def glob_metric_names(self, glob):
        """See the real Accessor for a description."""
        super(_ElasticSearchAccessor, self).glob_metric_names(glob)
        return iter(self.__glob_names(self._metric_names, glob))

    def glob_directory_names(self, glob):
        """See the real Accessor for a description."""
        super(_ElasticSearchAccessor, self).glob_directory_names(glob)
        return iter(self.__glob_names(self._directory_names, glob))

    def has_metric(self, metric_name):
        """See bg_accessor.Accessor."""
        super(_ElasticSearchAccessor, self).has_metric(metric_name)
        return self.get_metric(metric_name) is not None

    def get_metric(self, metric_name, touch=False):
        """See the real Accessor for a description."""
        super(_ElasticSearchAccessor, self).get_metric(metric_name, touch=touch)
        metric_name = ".".join(_components_from_name(metric_name))
        return self._name_to_metric.get(metric_name)

    def fetch_points(self, metric, time_start, time_end, stage, aggregated=True):
        """See the real Accessor for a description."""
        super(_ElasticSearchAccessor, self).fetch_points(
            metric, time_start, time_end, stage
        )
        points = self._metric_to_points[(metric.name, stage)]
        rows = []
        for ts in points.irange(time_start, time_end):
            # A row is time_base_ms, time_offset_ms, value, count
            if stage.aggregated():
                row = self.Row(ts * 1000.0, 0, 0, float(points[ts][0]), points[ts][1])
            else:
                row = self.Row0(ts * 1000.0, 0, float(points[ts][0]))
            rows.append(row)

        query_results = [(True, rows)]
        time_start_ms = int(time_start) * 1000
        time_end_ms = int(time_end) * 1000
        return bg_accessor.PointGrouper(
            metric,
            time_start_ms,
            time_end_ms,
            stage,
            query_results,
            aggregated=aggregated,
        )

    def touch_metric(self, metric_name):
        """See the real Accessor for a description."""
        super(_ElasticSearchAccessor, self).touch_metric(metric_name)

        # TODO Implements the function
        log.warn("%s is not implemented" % self.touch_metric.__name__)
        pass

    def repair(self, *args, **kwargs):
        """See the real Accessor for a description."""
        super(_ElasticSearchAccessor, self).repair(*args, **kwargs)
        callback_on_progress = kwargs.pop("callback_on_progress")

        def _callback(m, i, t):
            callback_on_progress(i, t)
            # TODO Implements the function
            log.warn("%s is not implemented" % self.repair.__name__)

        self.map(_callback, *args, **kwargs)

    def clean(self, *args, **kwargs):
        """See bg_accessor.Accessor."""
        super(_ElasticSearchAccessor, self).clean(*args, **kwargs)
        callback_on_progress = kwargs.pop("callback_on_progress")
        kwargs.pop("max_age", None)

        def _callback(m, i, t):
            callback_on_progress(i, t)
            # TODO Implements the function
            log.warn("%s is not implemented" % self.clean.__name__)

        self.map(_callback, *args, **kwargs)

    def map(
        self, callback, start_key=None, end_key=None, shard=0, nshards=1, errback=None
    ):
        """See bg_accessor.Accessor."""
        super(_ElasticSearchAccessor, self).map(
            callback, start_key, end_key, shard, nshards, errback
        )

        metrics = self._name_to_metric
        total = len(metrics)
        for i, metric in enumerate(metrics.values()):
            callback(metric, i, total)


def build(*args, **kwargs):
    """Return a bg_accessor.Accessor using ElasticSearch."""
    return _ElasticSearchAccessor(*args, **kwargs)
