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
import json
import logging
import os
import six
import elasticsearch
import elasticsearch_dsl
import time

from biggraphite import accessor as bg_accessor
from biggraphite import glob_utils as bg_glob
from biggraphite.drivers import _utils
from biggraphite.drivers import ttls

import prometheus_client

UPDATED_ON = prometheus_client.Summary(
    "bg_elasticsearch_updated_on_latency_seconds", "create latency in seconds"
)
READ_ON = prometheus_client.Summary(
    "bg_elasticsearch_read_on_latency_seconds", "create latency in seconds"
)

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

INDEX_DOC_TYPE = "_doc"

# TODO: Make that configurable (in a file), this will be particularly important
# for the number of shards and replicas.
INDEX_SETTINGS = {
    "settings": {
        "index": {
            "number_of_shards": 3,
            "number_of_replicas": 1,
            "refresh_interval": "60s",
            "translog": {
                "sync_interval": "120s",
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
        INDEX_DOC_TYPE: {
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
            "dynamic_templates": [
                {
                    "strings_as_keywords": {
                        "match": "p*",
                        "match_mapping_type": "string",
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
DEFAULT_HOSTS = ["127.0.0.1"]
DEFAULT_PORT = 9200
DEFAULT_TIMEOUT = 10
DEFAULT_USERNAME = os.getenv("BG_ELASTICSEARCH_USERNAME")
DEFAULT_PASSWORD = os.getenv("BG_ELASTICSEARCH_PASSWORD")

MAX_QUERY_SIZE = 10000

OPTIONS = {
    "username": str,
    "password": str,
    "index": str,
    "index_suffix": str,
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
    name = bg_accessor.sanitize_metric_name(metric.name)

    data = {
        "depth": len(components) - 1,
        "name": name,
    }

    for i, component in enumerate(components):
        data["p%d" % i] = component
    data.update({
        "uuid": metric.id,
        "created_on": metric.created_on or datetime.datetime.now(),
        "updated_on": metric.updated_on or datetime.datetime.now(),
        "read_on": metric.read_on or None,
        "config": config,
    })
    return data


class Error(bg_accessor.Error):
    """Base class for all exceptions from this module."""


class InvalidArgumentError(Error, bg_accessor.InvalidArgumentError):
    """Callee did not follow requirements on the arguments."""


def _parse_wildcard_component(component):
    """Given a complex component, this builds a wildcard constraint."""
    value = ""
    for subcomponent in component:
        if isinstance(subcomponent, bg_glob.AnySequence):
            value += "*"
        elif isinstance(subcomponent, six.string_types):
            value += subcomponent
        elif isinstance(subcomponent, bg_glob.AnyChar):
            value += '?'
        else:
            raise Error("Unhandled type '%s'" % subcomponent)
    return value


def _parse_regexp_component(component):
    """Given a complex component, this builds a regexp constraint."""
    if isinstance(component, bg_glob.Globstar):
        return ".*"
    regex = ""
    for subcomponent in component:
        if isinstance(subcomponent, bg_glob.Globstar):
            regex += ".*"
        elif isinstance(subcomponent, bg_glob.AnySequence):
            regex += "[^.]*"
        elif isinstance(subcomponent, six.string_types):
            regex += subcomponent
        elif isinstance(subcomponent, bg_glob.CharNotIn):
            regex += '[^' + ''.join(subcomponent.values) + ']'
        elif isinstance(subcomponent, bg_glob.CharIn):
            regex += '[' + ''.join(subcomponent.values) + ']'
        elif isinstance(subcomponent, bg_glob.SequenceIn):
            if subcomponent.negated:
                regex += '[^.]*'
            else:
                regex += '(' + '|'.join(subcomponent.values) + ')'
        elif isinstance(subcomponent, bg_glob.AnyChar):
            regex += '[^.]'
        else:
            raise Error("Unhandled type '%s'" % subcomponent)
    return regex


def parse_complex_component(component):
    """Given a complex component, this builds a constraint."""
    if all([
            any([
                 isinstance(sub_c, bg_glob.AnySequence),
                 isinstance(sub_c, bg_glob.AnyChar),
                 isinstance(sub_c, six.string_types),
            ]) for sub_c in component
            ]):
        return 'wildcard', _parse_wildcard_component(component)
    return 'regexp', _parse_regexp_component(component)


def _contains_regexp_wildcard(values):
    return any("*" in value for value in values)


def parse_simple_component(component):
    """Given a component with a simple type, this builds a constraint."""
    value = component[0]
    if isinstance(value, bg_glob.AnySequence):
        return None, None  # No constrain
    elif isinstance(value, six.string_types):
        return 'term', value
    elif isinstance(value, bg_glob.CharNotIn):
        return 'regexp', '[^' + ''.join(value.values) + ']'
    elif isinstance(value, bg_glob.CharIn):
        return 'regexp', '[' + ''.join(value.values) + ']'
    elif isinstance(value, bg_glob.SequenceIn):
        if _contains_regexp_wildcard(value.values):
            return 'regexp', '(' + '|'.join(value.values) + ')'
        else:
            return 'terms', value.values
    elif isinstance(value, bg_glob.AnyChar):
        return 'wildcard', '?'
    else:
        raise Error("Unhandled type '%s'" % value)


def _get_depth_from_components(components):
    return len(components) - 1


def _raise_unsupported():
    raise NotImplementedError("Elasticsearch accessor does not support data operations")


class _ElasticSearchAccessor(bg_accessor.Accessor):
    """A ElasticSearch acessor that doubles as a ElasticSearch MetadataCache."""

    Row = collections.namedtuple(
        "Row", ["time_start_ms", "offset", "shard", "value", "count"]
    )

    Row0 = collections.namedtuple("Row", ["time_start_ms", "offset", "value"])

    def __init__(
        self,
        hosts=DEFAULT_HOSTS,
        port=DEFAULT_PORT,
        index=DEFAULT_INDEX,
        index_suffix=DEFAULT_INDEX_SUFFIX,
        username=DEFAULT_USERNAME,
        password=DEFAULT_PASSWORD,
        timeout=DEFAULT_TIMEOUT,
        updated_on_ttl_sec=ttls.DEFAULT_UPDATED_ON_TTL_SEC,
        read_on_ttl_sec=ttls.DEFAULT_READ_ON_TTL_SEC,
    ):
        """Create a new ElasticSearchAccessor."""
        super(_ElasticSearchAccessor, self).__init__("ElasticSearch")
        self._hosts = list(hosts)
        self._port = port
        self._index_prefix = index
        self._index_suffix = index_suffix
        self._username = username
        self._password = password
        self._timeout = timeout
        self._known_indices = {}
        self.__glob_parser = bg_glob.GraphiteGlobParser()
        self.__updated_on_ttl_sec = updated_on_ttl_sec
        self.__read_on_ttl_sec = read_on_ttl_sec
        self.client = None
        log.debug(
            "Created Elasticsearch accessor with index prefix: '%s' and index suffix: '%s'" %
            (self._index_prefix, self._index_suffix)
        )

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

        kwargs = {
            'sniff_on_start': True,
            'sniff_on_connection_fail': True,
            'retry_on_timeout': True,
            'max_retries': 3,
            'timeout': self._timeout,
        }
        if self._port:
            kwargs['port'] = self._port
        if http_auth:
            kwargs['http_auth'] = http_auth

        es = elasticsearch.Elasticsearch(
            self._hosts,
            **kwargs
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
        pass

    def flush(self):
        """Flush any internal buffers."""
        if self.client:
            self.client.indices.flush(
                index="%s*" % self._index_prefix,
                allow_no_indices=True,
                ignore_unavailable=True,
                wait_if_ongoing=True,
            )
            self.client.indices.refresh(
                index="%s*" % self._index_prefix,
                allow_no_indices=True,
                ignore_unavailable=True,
            )

    def clear(self):
        """Clear all internal data."""
        self._known_indices = {}

    def get_index(self, metric):
        """Get the index where a metric should be stored."""
        # Here the index could be sharded further by looking at the
        # metric metadata, for example, per owner.
        index_name = self._index_prefix + datetime.datetime.now().strftime(self._index_suffix)
        if index_name not in self._known_indices:
            if not self.client.indices.exists(index=index_name):
                self.client.indices.create(
                    index=index_name,
                    body=INDEX_SETTINGS,
                    ignore=409
                )
                self.client.indices.flush()
            self._known_indices[index_name] = True

        return index_name

    def insert_points_async(self, metric, datapoints, on_done=None):
        """See the real Accessor for a description."""
        super(_ElasticSearchAccessor, self).insert_points_async(
            metric, datapoints, on_done
        )
        _raise_unsupported()

    def insert_downsampled_points_async(self, metric, datapoints, on_done=None):
        """See the real Accessor for a description."""
        super(_ElasticSearchAccessor, self).insert_downsampled_points_async(
            metric, datapoints, on_done
        )
        _raise_unsupported()

    def drop_all_metrics(self, *args, **kwargs):
        """See the real Accessor for a description."""
        super(_ElasticSearchAccessor, self).drop_all_metrics(*args, **kwargs)
        # Drop indices.
        self.client.indices.delete("%s*" % self._index_prefix)

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

        name = bg_accessor.sanitize_metric_name(name)
        metric = self.get_metric(name)

        if metric is None:
            raise InvalidArgumentError("Unknown metric '%s'" % name)

        updated_metric = self.make_metric(
            name,
            updated_metadata,
            created_on=metric.created_on,
            updated_on=datetime.datetime.now(),
            read_on=metric.read_on
        )
        self.create_metric(updated_metric)

    def delete_metric(self, name):
        name = bg_accessor.sanitize_metric_name(name)

        query = self._create_search_query() \
            .filter('term', name=name)

        log.debug(json.dumps(query.to_dict()))
        query.delete()

    def delete_directory(self, name):
        components = _components_from_name(name)
        depth = _get_depth_from_components(components)

        query = self._create_search_query()
        for index, component in enumerate(components):
            query = query.filter('term', **{"p%d" % index: component})
        query = query.filter('range', depth={'gte': depth})

        log.debug(json.dumps(query.to_dict()))
        query.delete()

    # TODO (t.chataigner) Add unittest.
    def _search_metrics_from_components(self, glob, components):
        search = self._create_search_query().source('name')

        # Handle glob with globstar(s).
        globstars = components.count(bg_glob.Globstar())
        if globstars:
            name_regexp = "\\.".join([_parse_regexp_component(c) for c in components])
            return True, search.filter('regexp', **{"name": name_regexp})

        # TODO (t.chataigner) Handle fully defined prefix (like a.b.c.*.*.*)
        # with a wildcard on name.

        # Handle fully defined glob.
        if self.__glob_parser.is_fully_defined(components):
            return False, search.filter(
                'term', **{"name": bg_accessor.sanitize_metric_name(glob)})

        # Handle all other use cases.
        for i, c in enumerate(components):
            if len(c) == 1:
                filter_type, value = parse_simple_component(c)
            else:
                filter_type, value = parse_complex_component(c)

            if filter_type:
                search = search.filter(filter_type, **{"p%d" % i: value})
        return False, search

    def glob_metric_names(self, glob):
        """See the real Accessor for a description."""
        super(_ElasticSearchAccessor, self).glob_metric_names(glob)
        if glob == "":
            return []

        components = self.__glob_parser.parse(glob)
        glob_depth = _get_depth_from_components(components)
        has_globstar, search = self._search_metrics_from_components(glob, components)
        if has_globstar:
            search = search.filter('range', depth={'gte': glob_depth})
        else:
            search = search.filter('term', depth=glob_depth)
        search = search.extra(from_=0, size=MAX_QUERY_SIZE)

        # TODO (t.chataigner) try to move the sort in the ES search and return a generator.
        log.debug(json.dumps(search.to_dict()))
        results = [h.name for h in search.execute()]
        results.sort()
        return iter(results)

    def glob_directory_names(self, glob):
        """See the real Accessor for a description."""
        super(_ElasticSearchAccessor, self).glob_directory_names(glob)
        if glob == "":
            return []

        components = self.__glob_parser.parse(glob)
        # There are no "directory" documents, only "metric" documents. Hence appending the
        # AnySequence after the provided glob: we search for metrics under that path.
        has_globstar, search = self._search_metrics_from_components(
            glob,
            components + [[bg_glob.AnySequence()]]
        )
        if has_globstar:
            # TODO (t.chataigner) Add a log or raise exception.
            return []

        glob_depth = _get_depth_from_components(components)
        # Use (glob_depth + 1) to filter only directories and
        # exclude metrics whose depth is glob_depth.
        search = search.filter('range', depth={'gte': glob_depth + 1})
        search = search.extra(from_=0, size=0)  # Do not return metrics.

        search.aggs.bucket('distinct_dirs', 'terms', field="p%d" % glob_depth, size=MAX_QUERY_SIZE)

        log.debug(json.dumps(search.to_dict()))
        response = search.execute()

        # This may not be the same behavior as other drivers.
        # It returns the glob with the list of possible last component for a directory.
        # It doesn't return the list of fully defined directory names.
        if "distinct_dirs" not in response.aggregations:
            # This happend when there is no index to search for the query.
            return []
        buckets = response.aggregations.distinct_dirs.buckets
        if glob_depth == 0:
            results = [b.key for b in buckets]
        else:
            glob_base = glob.rsplit('.', 1)[0]
            results = ["%s.%s" % (glob_base, b.key) for b in buckets]
        results.sort()
        return iter(results)

    def has_metric(self, metric_name):
        """See bg_accessor.Accessor."""
        super(_ElasticSearchAccessor, self).has_metric(metric_name)
        return self.get_metric(metric_name) is not None

    def get_metric(self, metric_name):
        """See the real Accessor for a description."""
        super(_ElasticSearchAccessor, self).get_metric(metric_name)

        metric_name = bg_accessor.sanitize_metric_name(metric_name)

        document = self.__get_document(metric_name)
        if document is None:
            return None

        return self._document_to_metric(document)

    def _document_to_metric(self, document):
        metadata = bg_accessor.MetricMetadata.from_string_dict(
            document.config.to_dict()
        )
        # TODO: Have a look at dsl doc to avoid parsing strings to dates
        # https://github.com/elastic/elasticsearch-dsl-py/blob/master/docs/persistence.rst
        return self.make_metric(
            document.name,
            metadata,
            created_on=ttls.str_to_datetime(document.created_on),
            updated_on=ttls.str_to_datetime(document.updated_on),
            read_on=ttls.str_to_datetime(document.read_on)
        )

    def __get_document(self, metric_name):
        search = self._create_search_query() \
            .source(['uuid', 'name', 'config', 'created_on', 'updated_on', 'read_on']) \
            .filter('term', name=metric_name) \
            .sort({'updated_on': {'order': 'desc'}})

        log.debug(json.dumps(search.to_dict()))
        response = search[:1].execute()

        if response is None or response.hits.total == 0:
            return None

        return response.hits[0]

    def fetch_points(self, metric, time_start, time_end, stage, aggregated=True):
        """See the real Accessor for a description."""
        super(_ElasticSearchAccessor, self).fetch_points(
            metric, time_start, time_end, stage
        )
        self.__update_read_on_on_need(metric)
        return []

    @UPDATED_ON.time()
    def touch_metric(self, metric):
        """See the real Accessor for a description."""
        super(_ElasticSearchAccessor, self).touch_metric(metric)

        if not metric.updated_on:
            delta = self.__updated_on_ttl_sec + 1
        else:
            updated_on_timestamp = time.mktime(metric.updated_on.timetuple())
            delta = int(time.time()) - int(updated_on_timestamp)

        if delta >= self.__updated_on_ttl_sec:
            # Update Elasticsearch.
            metric_name = bg_accessor.sanitize_metric_name(metric.name)
            document = self.__get_document(metric_name)
            if document:
                self.__touch_document(document)
            # Make sure the caller also see the change without refreshing
            # the metric.
            metric.updated_on = datetime.datetime.now()

    def __touch_document(self, document):
        metric = self._document_to_metric(document)
        new_index = self.get_index(metric)
        if new_index == document.meta.index:
            self.__update_existing_document(document)
        else:
            metric.updated_on = datetime.datetime.now()
            self.create_metric(metric)

    def __update_existing_document(self, document):
        index = document.meta.index
        document_id = document.uuid
        updated_on = datetime.datetime.now()
        data = {
            "doc": {
                "updated_on": updated_on
            }
        }
        self.__update_document(data, index, document_id)
        document.updated_on = ttls.datetime_to_str(updated_on)

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

        # TODO: implement
        log.warn("map is not implemented")
        metrics = []
        total = len(metrics)
        for i, metric in enumerate(metrics):
            callback(metric, i, total)

    @READ_ON.time()
    def __update_read_on_on_need(self, metric):
        if not metric.read_on:
            delta = self.__read_on_ttl_sec + 1
        else:
            read_on_timestamp = ttls.str_to_timestamp(metric.read_on)
            delta = int(time.time()) - int(read_on_timestamp)

        if delta >= self.__read_on_ttl_sec:
            # TODO: execute asynchronously
            self.__update_read_on(metric)

    def __update_read_on(self, metric):
        # TODO: state if we should move the document from its index to
        # the current (today) index
        data = {
            "doc": {
                "read_on": datetime.datetime.now()
            }
        }
        index = self.get_index(metric.name)
        self.__update_document(data, index, metric.id)

    def __update_document(self, data, index, document_id):
        self.client.update(
            index=index,
            doc_type=INDEX_DOC_TYPE,
            id=document_id,
            body=data,
            ignore=404
        )

    def _create_search_query(self):
        return elasticsearch_dsl.Search() \
            .using(self.client) \
            .index("%s*" % self._index_prefix)


def build(*args, **kwargs):
    """Return a bg_accessor.Accessor using ElasticSearch."""
    return _ElasticSearchAccessor(*args, **kwargs)
