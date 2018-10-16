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
import time

from concurrent import futures

import elasticsearch
import elasticsearch_dsl
import prometheus_client
import six

from biggraphite import accessor as bg_accessor, tracing
from biggraphite import glob_utils as bg_glob
from biggraphite import metric as bg_metric
from biggraphite.drivers import _utils
from biggraphite.drivers import ttls

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

DEFAULT_INDEX = "biggraphite_metrics"
DEFAULT_INDEX_SUFFIX = "_%Y-%m-%d"
DEFAULT_HOSTS = ["127.0.0.1"]
DEFAULT_PORT = 9200
DEFAULT_TIMEOUT = 10
DEFAULT_USERNAME = os.getenv("BG_ELASTICSEARCH_USERNAME")
DEFAULT_PASSWORD = os.getenv("BG_ELASTICSEARCH_PASSWORD")
DEFAULT_READ_ON_SAMPLING_RATE = 0.05

DEFAULT_ES_SCHEMA_PATH = os.path.join(
    os.path.dirname(__file__), "elasticsearch_schema.json"
)

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
        "--elasticsearch_username",
        help="elasticsearch username.",
        default=''
    )
    parser.add_argument(
        "--elasticsearch_password",
        help="elasticsearch password.",
        default=''
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
    parser.add_argument(
        "--elasticsearch_schema",
        metavar="SCHEMA",
        help="elasticsearch schema path.",
        default=DEFAULT_ES_SCHEMA_PATH,
    )


def _components_from_name(metric_name):
    res = metric_name.split(".")
    return list(filter(None, res))


def document_from_metric(metric):
    """Creates an ElasticSearch document from a Metric."""
    config = metric.metadata.as_string_dict()
    components = _components_from_name(metric.name)
    name = bg_metric.sanitize_metric_name(metric.name)

    data = {"depth": len(components) - 1, "name": name}

    for i, component in enumerate(components):
        data["p%d" % i] = component
    data.update(
        {
            "uuid": metric.id,
            "created_on": metric.created_on or datetime.datetime.now(),
            "updated_on": metric.updated_on or datetime.datetime.now(),
            "read_on": metric.read_on or None,
            "config": config,
        }
    )
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
            value += "?"
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
            regex += "[^" + "".join(subcomponent.values) + "]"
        elif isinstance(subcomponent, bg_glob.CharIn):
            regex += "[" + "".join(subcomponent.values) + "]"
        elif isinstance(subcomponent, bg_glob.SequenceIn):
            if subcomponent.negated:
                regex += "[^.]*"
            else:
                regex += "(" + "|".join(subcomponent.values) + ")"
        elif isinstance(subcomponent, bg_glob.AnyChar):
            regex += "[^.]"
        else:
            raise Error("Unhandled type '%s'" % subcomponent)
    return regex


def parse_complex_component(component):
    """Given a complex component, this builds a constraint."""
    if all(
        [
            any(
                [
                    isinstance(sub_c, bg_glob.AnySequence),
                    isinstance(sub_c, bg_glob.AnyChar),
                    isinstance(sub_c, six.string_types),
                ]
            )
            for sub_c in component
        ]
    ):
        return "wildcard", _parse_wildcard_component(component)
    return "regexp", _parse_regexp_component(component)


def _contains_regexp_wildcard(values):
    return any("*" in value for value in values)


def parse_simple_component(component):
    """Given a component with a simple type, this builds a constraint."""
    value = component[0]
    if isinstance(value, bg_glob.AnySequence):
        return None, None  # No constrain
    elif isinstance(value, six.string_types):
        return "term", value
    elif isinstance(value, bg_glob.CharNotIn):
        return "regexp", "[^" + "".join(value.values) + "]"
    elif isinstance(value, bg_glob.CharIn):
        return "regexp", "[" + "".join(value.values) + "]"
    elif isinstance(value, bg_glob.SequenceIn):
        if _contains_regexp_wildcard(value.values):
            return "regexp", "(" + "|".join(value.values) + ")"
        else:
            return "terms", value.values
    elif isinstance(value, bg_glob.AnyChar):
        return "wildcard", "?"
    else:
        raise Error("Unhandled type '%s'" % value)


def _get_depth_from_components(components):
    return len(components) - 1


def _raise_unsupported():
    raise NotImplementedError("Elasticsearch accessor does not support data operations")


class _ElasticSearchAccessor(bg_accessor.Accessor):
    """A ElasticSearch acessor that doubles as a ElasticSearch MetadataCache."""

    TYPE = "elasticsearch"

    def __init__(
        self,
        hosts=DEFAULT_HOSTS,
        port=DEFAULT_PORT,
        index=DEFAULT_INDEX,
        index_suffix=DEFAULT_INDEX_SUFFIX,
        username=None,
        password=None,
        timeout=DEFAULT_TIMEOUT,
        updated_on_ttl_sec=ttls.DEFAULT_UPDATED_ON_TTL_SEC,
        read_on_ttl_sec=ttls.DEFAULT_READ_ON_TTL_SEC,
        read_on_sampling_rate=DEFAULT_READ_ON_SAMPLING_RATE,
        schema_path=DEFAULT_ES_SCHEMA_PATH,
    ):
        """Create a new ElasticSearchAccessor."""
        super(_ElasticSearchAccessor, self).__init__("ElasticSearch")
        self._hosts = list(hosts)
        self._port = port
        self._index_prefix = index
        self._index_suffix = index_suffix
        self._username = username or DEFAULT_USERNAME
        self._password = password or DEFAULT_PASSWORD
        self._timeout = timeout
        self._known_indices = {}
        self.__glob_parser = bg_glob.GraphiteGlobParser()
        self.__updated_on_ttl_sec = updated_on_ttl_sec
        self.__read_on_ttl_sec = read_on_ttl_sec
        self.__read_on_counter = 0
        self.__read_on_sampling_rate = read_on_sampling_rate
        self.__schema_path = schema_path
        self.client = None
        self.schema = None
        log.debug(
            "Created Elasticsearch accessor with index prefix: '%s' and index suffix: '%s'"
            % (self._index_prefix, self._index_suffix)
        )
        # This will be used for low-priority background operations.
        self._executor = None

    @tracing.trace
    def connect(self, *args, **kwargs):
        """See the real Accessor for a description."""
        super(_ElasticSearchAccessor, self).connect(*args, **kwargs)
        self._connect()
        self.is_connected = True

    def _connect(self):
        """Connect to elasticsearch."""
        if self.is_connected:
            return
        if not self._executor:
            # It has a single thread because it's really for low-priority operations.
            self._executor = futures.ThreadPoolExecutor(
                max_workers=1, thread_name_prefix="bg_es"
            )

        if self._username:
            http_auth = (self._username, self._password or "")
        else:
            http_auth = None

        kwargs = {
            "sniff_on_start": True,
            "sniff_on_connection_fail": True,
            "retry_on_timeout": True,
            "max_retries": 3,
            "timeout": self._timeout,
        }
        if self._port:
            kwargs["port"] = self._port
        if http_auth:
            kwargs["http_auth"] = http_auth

        es = elasticsearch.Elasticsearch(self._hosts, **kwargs)

        log.info("Connected: %s" % es.info())
        self.client = es

        with open(self.__schema_path, "r") as es_schema_file:
            self.schema = json.load(es_schema_file)

    @tracing.trace
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
        if self._executor:
            self._executor.shutdown()
            self._executor = None

    def background(self):
        """Perform periodic background operations."""
        pass

    def flush(self):
        """Flush any internal buffers."""
        # First, flush background tasks.
        if self._executor:
            # Since we have a single thread, it's cheap way to join()
            future = self._executor.submit(lambda: True)
            future.result(timeout=5)

        # Then, make sure we can read everything we wrote.
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

    def get_index(self, metric):
        """Get the index where a metric should be stored."""
        # Here the index could be sharded further by looking at the
        # metric metadata, for example, per owner.
        index_name = self._index_prefix + datetime.datetime.now().strftime(
            self._index_suffix
        )
        if index_name not in self._known_indices:
            if not self.client.indices.exists(index=index_name):
                self.client.indices.create(
                    index=index_name, body=self.schema, ignore=409
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
        self._known_indices = {}

    @tracing.trace
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

    @tracing.trace
    def update_metric(self, name, updated_metadata):
        """See bg_accessor.Accessor."""
        super(_ElasticSearchAccessor, self).update_metric(name, updated_metadata)

        name = bg_metric.sanitize_metric_name(name)
        metric = self.get_metric(name)

        if metric is None:
            raise InvalidArgumentError("Unknown metric '%s'" % name)

        updated_metric = bg_metric.make_metric(
            name,
            updated_metadata,
            created_on=metric.created_on,
            updated_on=datetime.datetime.now(),
            read_on=metric.read_on,
        )
        self.create_metric(updated_metric)

    @tracing.trace
    def delete_metric(self, name):
        name = bg_metric.sanitize_metric_name(name)

        query = self._create_search_query().filter("term", name=name)

        log.debug(json.dumps(query.to_dict(), default=str))
        query.delete()

    @tracing.trace
    def delete_directory(self, name):
        components = _components_from_name(name)
        depth = _get_depth_from_components(components)

        query = self._create_search_query()
        for index, component in enumerate(components):
            query = query.filter("term", **{"p%d" % index: component})
        query = query.filter("range", depth={"gte": depth})

        log.debug(json.dumps(query.to_dict(), default=str))
        query.delete()

    # TODO (t.chataigner) Add unittest.
    def _search_metrics_from_components(self, glob, components, search=None):
        search = self._create_search_query() if search is None else search

        # Handle glob with globstar(s).
        globstars = components.count(bg_glob.Globstar())
        if globstars:
            name_regexp = "\\.".join([_parse_regexp_component(c) for c in components])
            return True, search.filter("regexp", **{"name": name_regexp})

        # TODO (t.chataigner) Handle fully defined prefix (like a.b.c.*.*.*)
        # with a wildcard on name.

        # Handle fully defined glob.
        if self.__glob_parser.is_fully_defined(components):
            return (
                False,
                search.filter("term", **{"name": bg_metric.sanitize_metric_name(glob)}),
            )

        # Handle all other use cases.
        for i, c in enumerate(components):
            if len(c) == 1:
                filter_type, value = parse_simple_component(c)
            else:
                filter_type, value = parse_complex_component(c)

            if filter_type:
                search = search.filter(filter_type, **{"p%d" % i: value})
        return False, search

    def glob_metrics(self, glob, start_time=None, end_time=None):
        """Return a sorted list of metrics matching this glob."""
        super(_ElasticSearchAccessor, self).glob_metrics(glob, start_time, end_time)

        if glob == "":
            return []

        components = self.__glob_parser.parse(glob)
        glob_depth = _get_depth_from_components(components)
        search = self._create_search_query(start_time, end_time)

        has_globstar, search = self._search_metrics_from_components(
            glob, components, search
        )
        if has_globstar:
            search = search.filter("range", depth={"gte": glob_depth})
        else:
            search = search.filter("term", depth=glob_depth)
        search = search.extra(from_=0, size=MAX_QUERY_SIZE)

        # TODO (t.chataigner) try to move the sort in the ES search and return a generator.
        log.debug(json.dumps(search.to_dict(), default=str))

        documents = search.execute()
        results = [self._document_to_metric(document)
                   for document in self._deduplicate_documents(documents)]
        results.sort(key=lambda metric: metric.name)

        return results

    @staticmethod
    def _deduplicate_documents(documents):
        """Deduplicate documents with same name over several indices to keep most recent one."""
        documents_by_name = dict()
        docs = list(documents)
        docs.sort(key=lambda doc: doc.updated_on, reverse=True)

        for document in docs:
            if document.name not in documents_by_name:
                documents_by_name[document.meta.id] = document
        return documents_by_name.values()

    def glob_metric_names(self, glob, start_time=None, end_time=None):
        """See the real Accessor for a description."""
        super(_ElasticSearchAccessor, self).glob_metric_names(
            glob, start_time, end_time
        )
        results = [h.name for h in self.glob_metrics(glob, start_time, end_time)]
        return iter(results)

    @tracing.trace
    def glob_directory_names(self, glob, start_time=None, end_time=None):
        """See the real Accessor for a description."""
        super(_ElasticSearchAccessor, self).glob_directory_names(
            glob, start_time, end_time
        )
        if glob == "":
            return []

        components = self.__glob_parser.parse(glob)
        search = self._create_search_query(start_time, end_time)
        # There are no "directory" documents, only "metric" documents. Hence appending the
        # AnySequence after the provided glob: we search for metrics under that path.
        has_globstar, search = self._search_metrics_from_components(
            glob, components + [[bg_glob.AnySequence()]], search
        )
        if has_globstar:
            # TODO (t.chataigner) Add a log or raise exception.
            return []

        glob_depth = _get_depth_from_components(components)
        # Use (glob_depth + 1) to filter only directories and
        # exclude metrics whose depth is glob_depth.
        search = search.filter("range", depth={"gte": glob_depth + 1})
        search = search.extra(from_=0, size=0)  # Do not return metrics.

        search.aggs.bucket(
            "distinct_dirs", "terms", field="p%d" % glob_depth, size=MAX_QUERY_SIZE
        )

        log.debug(json.dumps(search.to_dict(), default=str))
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
            glob_base = glob.rsplit(".", 1)[0]
            results = ["%s.%s" % (glob_base, b.key) for b in buckets]
        results.sort()
        return iter(results)

    @tracing.trace
    def has_metric(self, metric_name):
        """See bg_accessor.Accessor."""
        super(_ElasticSearchAccessor, self).has_metric(metric_name)
        return self.get_metric(metric_name) is not None

    @tracing.trace
    def get_metric(self, metric_name):
        """See the real Accessor for a description."""
        super(_ElasticSearchAccessor, self).get_metric(metric_name)
        metric_name = bg_metric.sanitize_metric_name(metric_name)

        document = self.__get_document(metric_name)
        if document is None:
            return None

        return self._document_to_metric(document)

    def _document_to_metric(self, document):
        metadata = bg_metric.MetricMetadata.from_string_dict(document.config.to_dict())
        # TODO: Have a look at dsl doc to avoid parsing strings to dates
        # https://github.com/elastic/elasticsearch-dsl-py/blob/master/docs/persistence.rst
        return bg_metric.make_metric(
            document.name,
            metadata,
            created_on=ttls.str_to_datetime(document.created_on),
            updated_on=ttls.str_to_datetime(document.updated_on),
            read_on=ttls.str_to_datetime(document.read_on),
        )

    def __get_document(self, metric_name, index=None):
        search = (
            self._create_search_query(index=index)
            .source(["uuid", "name", "config", "created_on", "updated_on", "read_on"])
            .filter("term", name=metric_name)
            .sort({"updated_on": {"order": "desc"}})
        )

        log.debug(json.dumps(search.to_dict(), default=str))

        response = search[:1].execute()

        if response is None or response.hits.total == 0:
            return None

        return response.hits[0]

    @tracing.trace
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
            # Make sure the caller also see the change without refreshing
            # the metric.
            metric.updated_on = datetime.datetime.now()
            self._executor.submit(self.__touch_document, metric, metric.updated_on)

    def __touch_document(self, metric, updated_on):
        # Update Elasticsearch.
        metric_name = bg_metric.sanitize_metric_name(metric.name)

        # See if the metric is in the correct index
        index = self.get_index(metric)
        document = self.__get_document(metric_name, index)

        if not document:
            # Re-create the metric.
            metric.updated_on = updated_on
            self.create_metric(metric)
        else:
            self.__update_existing_document(document, updated_on)

    def __update_existing_document(self, document, updated_on):
        index = document.meta.index
        document_id = document.uuid
        data = {"doc": {"updated_on": updated_on}}
        self.__update_document(data, index, document_id)
        document.updated_on = ttls.datetime_to_str(updated_on)

    def repair(self, *args, **kwargs):
        """See the real Accessor for a description."""
        super(_ElasticSearchAccessor, self).repair(*args, **kwargs)
        callback_on_progress = kwargs.pop("callback_on_progress", None)

        def _callback(m, i, t):
            if callback_on_progress:
                callback_on_progress(i, t)
            # TODO Implements the function
            log.warning("%s is not implemented" % self.repair.__name__)

        self.map(_callback, *args, **kwargs)

    def clean(
        self,
        max_age=None,
        start_key=None,
        end_key=None,
        shard=1,
        nshards=0,
        callback_on_progress=None,
    ):
        """See bg_accessor.Accessor."""
        super(_ElasticSearchAccessor, self).clean(
            max_age, start_key, end_key, shard, nshards, callback_on_progress
        )
        custom_suffix = ""
        index_suffix = self._index_suffix

        indices_datetime = []
        # TODO, we could also do this:
        # self.client.cat.indices(
        #   index="%s*" % self._index_prefix,
        #   format="json", h="index,creation.date"
        # )
        for index in self.client.indices.get("%s*" % self._index_prefix):
            # Hack if we use week as strptime cannot parse without weekday
            # So we add -0 to identify the first day of week
            if index_suffix.endswith("%W"):
                index_suffix += "-%w"
                custom_suffix = "-0"
            index += custom_suffix
            try:
                index_datetime = datetime.datetime.strptime(
                    index, self._index_prefix + index_suffix
                )
            except ValueError as e:
                # Ignore dates that we can't parse.
                logging.error("Can't parse %s: %s" % (index, e))
                continue
            indices_datetime.append(index_datetime)

        indices_datetime.sort(reverse=True)
        # Remove the two most recent index to be sure we keep data we are currently using.
        indices_datetime.pop(0)
        indices_datetime.pop(0)

        max_date = datetime.datetime.now() - datetime.timedelta(seconds=max_age)
        done = 0
        for index_datetime in indices_datetime:
            if index_datetime < max_date:
                index_to_drop = self._index_prefix + index_datetime.strftime(
                    self._index_suffix
                )

                if self.client.indices.exists(index_to_drop):
                    logging.info("Removing index %s" % index_to_drop)
                    self.client.indices.delete(index_to_drop)
                else:
                    # Maybe wrong suffix, should be deleted by hand.
                    logging.warning("Can't find %s" % index_to_drop)

            done += 1
            if callback_on_progress:
                callback_on_progress(done, len(indices_datetime))

    def map(
        self,
        callback,
        start_key=None,
        end_key=None,
        shard=0,
        nshards=1,
        errback=None,
        callback_on_progress=None,
    ):
        """See bg_accessor.Accessor."""
        super(_ElasticSearchAccessor, self).map(
            callback, start_key, end_key, shard, nshards, errback
        )

        # TODO: implement
        log.error("map is not implemented")
        metrics = []
        total = len(metrics)
        for i, metric in enumerate(metrics):
            callback(metric, i, total)

    def metric_stats(self, namespaces):
        """See bg_accessor.Accessor."""
        # TODO: implement sharding using this
        # https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-preference.html
        n_metrics = collections.defaultdict(int)
        n_points = collections.defaultdict(int)

        for pattern, namespace in namespaces.patterns.items():
            search = self._create_search_query()
            # Transform a pattern to a regexp.
            regexp = pattern.pattern + ".*"
            search = search.query("regexp", name=regexp)
            search.aggs.bucket(
                "metrics_per_retention", "terms", field="config.retention.keyword"
            )
            result = search.execute()
            if not result.aggregations:
                continue
            metrics_per_retention = result.aggregations.metrics_per_retention.buckets
            metrics_total = 0
            points = 0
            for metrics in metrics_per_retention:
                metrics_total += metrics.doc_count
                points += (
                    bg_metric.Retention.from_string(metrics.key).points
                    * metrics.doc_count
                )

            n_metrics[namespace] = metrics_total
            n_points[namespace] = points

        return n_metrics, n_points

    @READ_ON.time()
    def __update_read_on_on_need(self, metric):
        # TODO: remove the sampling rate once graphite.py stops using a cache
        # (that doesn't get updated when we updated read_on). Instead we
        # should collect the latest read_on when we list metrics.
        rate = int(1 / self.__read_on_sampling_rate)

        skip = self.__read_on_counter % rate > 0
        self.__read_on_counter += 1

        if skip:
            return

        if not metric.read_on:
            delta = self.__read_on_ttl_sec + 1
        else:
            read_on_timestamp = ttls.str_to_timestamp(metric.read_on)
            delta = int(time.time()) - int(read_on_timestamp)

        if delta >= self.__read_on_ttl_sec:
            self._executor.submit(self.__update_read_on, metric)
            # Make sure the caller also see the change without refreshing
            # the metric.
            metric.read_on = datetime.datetime.now()

    def __update_read_on(self, metric):
        data = {"doc": {"read_on": datetime.datetime.now()}}

        # Get the latest version of the metric and update it.
        document = self.__get_document(metric.name)
        self.__update_document(data, document.meta.index, metric.id)

    def __update_document(self, data, index, document_id):
        self.client.update(
            index=index,
            doc_type=INDEX_DOC_TYPE,
            id=document_id,
            body=data,
            ignore=[404, 409],
        )

    def _create_search_query(self, start_time=None, end_time=None, index=None):
        if not index:
            index = "%s*" % self._index_prefix
        search = (
            elasticsearch_dsl.Search()
            .using(self.client)
            .index(index)
        )

        if start_time is not None:
            # `updated_on` field has a delay before it is updated, so a metric can still be active
            # without having this value up to date.
            #
            # Example:
            #
            #      current `updated_on`         future `updated_on` (if exists)
            #         |                          |
            #         < ---- updated_on_ttl ---- >
            #         |                          |
            # ---------------------------------------------> t
            #                   |              |
            #             query start_time     |
            #                                 now
            #
            # To find metrics in this case, we need to take into account that TTL in the query:
            #
            #   updated_on >= start_time - ttl
            #
            updated_on_lower_bound = start_time - datetime.timedelta(
                seconds=self.__updated_on_ttl_sec
            )
            search = search.filter(
                "range", updated_on={"gte": updated_on_lower_bound.isoformat()}
            )
        if end_time is not None:
            search = search.filter("range", created_on={"lte": end_time.isoformat()})
        return search


def build(*args, **kwargs):
    """Return a bg_accessor.Accessor using ElasticSearch."""
    return _ElasticSearchAccessor(*args, **kwargs)
