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
"""Adapter between BigGraphite and Graphite."""
from __future__ import absolute_import  # Otherwise graphite is this module.

import time
import threading

from graphite import intervals
from graphite import node
from graphite import readers
from graphite import carbonlink
from graphite.logger import log
from graphite.render import hashing

from biggraphite import accessor as bg_accessor
from biggraphite import accessor_cache as bg_accessor_cache
from biggraphite import glob_utils
from biggraphite import graphite_utils


_CONFIG_NAME = "biggraphite"

try:
    from graphite.readers import utils as readers_utils
    BaseReader = readers_utils.BaseReader
except ImportError:
    BaseReader = object
try:
    from graphite.finders import utils as finders_utils
    BaseFinder = finders_utils.BaseFinder
except ImportError:
    BaseFinder = object
try:
    FetchInProgress = readers.FetchInProgress
except AttributeError:
    FetchInProgress = None


class Error(Exception):
    """Base class for all exceptions from this module."""


_DISABLED = 'DISABLED'


class Reader(BaseReader):
    """As per the Graphite API, fetches points for and metadata for a given metric."""

    __slots__ = (
        "_accessor", "_metadata_cache", "_carbonlink",
        "_metric", "_metric_name", )

    def __init__(self, accessor, metadata_cache, carbonlink, metric_name):
        """Create a new reader."""
        assert accessor
        assert metadata_cache

        self._accessor = accessor
        self._metadata_cache = metadata_cache
        self._carbonlink = carbonlink
        self._metric = None
        self._metric_name = metric_name

    def __get_cached_datapoints(self, stage):
        cached_datapoints = []
        try:
            # TODO: maybe this works with non stage0 now, need to check.
            if stage.stage0 and self._carbonlink:
                cached_datapoints = self._carbonlink.query(self._metric_name)
        except Exception:
            log.exception("Failed CarbonLink query '%s'" % self._metric_name)
        return cached_datapoints

    def __get_metadata(self):
        self.__refresh_metric()
        if self._metric is not None:
            return self._metric.metadata
        else:
            return bg_accessor.MetricMetadata()

    def __get_time_info(self, start_time, end_time, now, shift=False):
        """Constrain the provided range in an aligned interval within retention."""
        retention = (
            self.__get_metadata().retention or
            bg_accessor.MetricMetadata._DEFAULT_RETENTION
        )
        return retention.align_time_window(
            start_time, end_time, now, shift=shift)

    def __refresh_metric(self):
        """Set self._metric."""
        # TODO: This can be blocking, so we need to do one of:
        # - forward metadata to the constructor (possibly by adding mget
        #   to the metadata cache and the accessor)
        # - make fetch_async really async
        # - use a thread pool in `fetch_multi`
        if self._metric is None:
            self._metric = self._metadata_cache.get_metric(self._metric_name)

    def _merge_cached_points(self, stage, start, step, aggregation_method,
                             points, cached_datapoints, raw_step=None):
        """Merge points from carbonlink with points from database."""
        if isinstance(cached_datapoints, dict):
            cached_datapoints = list(cached_datapoints.items())

        return readers.utils.merge_with_cache(
            cached_datapoints, start, step, points,
            func=aggregation_method, raw_step=raw_step
        )

    def fetch_async(self, start_time, end_time, now=None, requestContext=None):
        """Fetch point for a given interval as per the Graphite API.

        Args:
          start_time: Timestamp to fetch points from, will constrained by retention policy.
          end_time: Timestamp to fetch points until, will constrained by retention policy.
          now: Current timestamp as a float, defaults to time.time(), for tests.

        Returns:
          A callable that returns a tuple made of (rounded start time,
          rounded end time, stage precision), points
          Points is a list for which missing points are set to None.
        """
        fetch_start = time.time()
        log.rendering('fetch(%s, %d, %d) - start' % (
            self._metric_name, start_time, end_time))

        self.__refresh_metric()
        if now is None:
            now = time.time()

        metadata = self.__get_metadata()
        start_time, end_time, stage = self.__get_time_info(
            start_time, end_time, now)
        start_step = stage.step(start_time)
        points_num = stage.step(end_time) - start_step
        step = stage.precision
        aggregation_method = metadata.aggregator.carbon_name
        raw_step = metadata.retention.stage0.precision

        if not self._metric:
            # The metric doesn't exist, let's fail gracefully.
            ts_and_points = []
        else:
            # This returns a generator which we can iterate on later.
            ts_and_points = self._accessor.fetch_points(
                self._metric, start_time, end_time, stage)

        def read_points():
            read_start = time.time()

            cached_datapoints = self.__get_cached_datapoints(stage)

            # TODO: Consider wrapping an array (using NaN for None) for
            # speed&memory efficiency
            points = [None] * points_num
            for ts, point in ts_and_points:
                index = stage.step(ts) - start_step
                points[index] = point

            if cached_datapoints:
                points = self._merge_cached_points(
                    stage, start_step, step, aggregation_method,
                    points, cached_datapoints, raw_step=raw_step
                )

            now = time.time()
            log.rendering(
                'fetch(%s, %d, %d) - %d points - read: %f secs - total: %f secs' % (
                    self._metric_name, start_time, end_time, len(points),
                    now - read_start, now - fetch_start))
            return (start_time, end_time, stage.precision), points

        log.rendering('fetch(%s, %d, %d) - started' % (
            self._metric_name, start_time, end_time))

        return read_points

    def fetch(self, start_time, end_time, now=None, requestContext=None):
        """Fetch point for a given interval as per the Graphite API.

        Args:
          start_time: Timestamp to fetch points from, will constrained by retention policy.
          end_time: Timestamp to fetch points until, will constrained by retention policy.
          now: Current timestamp as a float, defaults to time.time(), for tests.

        Returns:
          A tuple made of (rounded start time, rounded end time, stage precision), points
          Points is a list for which missing points are set to None.
        """
        results = self.fetch_async(start_time, end_time, now=now, requestContext=None)
        if FetchInProgress:
            return FetchInProgress(results)
        else:
            # That is currently really slow. We need to implement fetch_multi
            # to avoid that.
            return results()

    def get_intervals(self, now=None):
        """Fetch information on the retention policy, as per the Graphite API.

        Args:
          now: Current timestamp as a float, defaults to time.time(), for tests.

        Returns:
          A list of interval.Intervals for which we have data.
        """
        if now is None:
            now = time.time()

        # Call __get_time_info with the widest conceivable range will make it be
        # shortened to the widest range available according to retention policy.
        start, end, unused_stage = self.__get_time_info(
            0, now, now, shift=True)
        return intervals.IntervalSet([intervals.Interval(start, end)])


class Finder(BaseFinder):
    """Finder plugin for BigGraphite."""

    local = False

    def __init__(self, directories=None, accessor=None,
                 metadata_cache=None, carbonlink=None):
        """Build a new finder.

        Args:
          directories: Ignored (here only for compatibility)
          accessor: Accessor for injection (e.g. for testing, not used by Graphite)
          metadata_cache: Cache (for injection)
          carbonlink: Carbonlink (for injection)
        """
        self._accessor = accessor
        self._cache = metadata_cache
        self._carbonlink = carbonlink
        self._django_cache = None
        self._cache_timeout = None
        self._lock = threading.RLock()

    def accessor(self):
        """Return an accessor."""
        with self._lock:
            if not self._accessor:
                from django.conf import settings as django_settings
                accessor = graphite_utils.accessor_from_settings(
                    django_settings)
                # If connect() fail it will raise an exception that will be caught
                # by the caller. If the plugin is called again, self._accessor will
                # still be None and a new accessor will be created.
                try:
                    accessor.connect()
                except Exception as e:
                    log.exception("failed to connect()")
                    accessor.shutdown()
                    raise e
                accessor.set_cache(
                    bg_accessor_cache.DjangoCache(self.django_cache()),
                    metadata_ttl=django_settings.FIND_CACHE_DURATION,
                    data_ttl=django_settings.DEFAULT_CACHE_DURATION
                )
                self._accessor = accessor
        return self._accessor

    def carbonlink(self):
        """Return a carbonlink."""
        with self._lock:
            if not self._carbonlink:
                from django.conf import settings as django_settings
                if not django_settings.CARBONLINK_HOSTS:
                    self._carbonlink = _DISABLED
                elif callable(carbonlink.CarbonLink):
                    self._carbonlink = carbonlink.CarbonLink()
                else:
                    self._carbonlink = carbonlink.CarbonLink
        if self._carbonlink == _DISABLED:
            return None
        return self._carbonlink

    def cache(self):
        """Return a metadata cache."""
        with self._lock:
            if not self._cache:
                # TODO: Allow to use Django's cache.
                from django.conf import settings as django_settings
                cache = graphite_utils.cache_from_settings(
                    self.accessor(), django_settings)
                cache.open()
                self._cache = cache
        return self._cache

    def django_cache(self):
        """Return the django cache."""
        with self._lock:
            if not self._django_cache:
                from django.conf import settings as django_settings
                from django.core.cache import cache
                self._django_cache = cache
                self._cache_timeout = django_settings.FIND_CACHE_DURATION
        return self._django_cache

    def _hash(self, obj):
        # Make sure we use all the member of the objects to
        # build a unique key.
        return hashing.compactHash(str(sorted(vars(obj).items())))

    def find_nodes(self, query):
        """Find nodes matching a query."""
        # TODO: we should probably consider query.startTime and query.endTime
        #  to filter out metrics that had no points in this interval.
        leaves_only = hasattr(query, 'leaves_only') and query.leaves_only
        cache_key = "find_nodes:%s" % self._hash(query)
        cached = self.django_cache().get(cache_key)
        if cached is not None:
            cache_hit = True
            success, results = cached
        else:
            find_start = time.time()
            try:
                results = glob_utils.graphite_glob(
                    self.accessor(), query.pattern,
                    metrics=True, directories=not leaves_only
                )
                success = True
            except bg_accessor.Error as e:
                success = False
                results = e

            log.rendering(
                'find(%s) - %f secs' % (query.pattern, time.time() - find_start))
            cache_hit = False

        if not cache_hit:
            self.django_cache().set(cache_key, (success, results), self._cache_timeout)

        if not success:
            raise results

        metric_names, directories = results

        for metric_name in metric_names:
            reader = Reader(
                self.accessor(), self.cache(), self.carbonlink(), metric_name
            )
            yield node.LeafNode(metric_name, reader)

        for directory in directories:
            yield node.BranchNode(directory)

    def fetch(self, patterns, start_time, end_time, now=None, requestContext=None):
        """Fetch multiple patterns at once.

        This method is used to fetch multiple patterns at once, this
        allows alternate finders to do batching on their side when they can.

        Returns:
          an iterable of
          {
            'pathExpression': pattern,
            'path': node.path,
            'time_info': time_info,
            'values': values,
          }
        """
        requestContext = requestContext or {}

        queries = [
            finders_utils.FindQuery(
                pattern, start_time, end_time,
                local=requestContext.get('localOnly'),
                headers=requestContext.get('forwardHeaders'),
                leaves_only=True,
            )
            for pattern in patterns
        ]

        # TODO: Implement a real way to fetch multiple nodes at once. This
        # means that we need to create accessor.fetch_point_sync(). Check
        # how the concurrent executor works before doing any work.

        # In order to be a little bit more efficient with Graphite 1.1.0 we
        # first schedule all the queries, then read the results as they come.
        queries_and_generators = []

        for n, query in self.find_multi(queries):
            if not isinstance(n, node.LeafNode):
                continue

            # Call directly the async method
            gen = n.reader.fetch_async(
                start_time, end_time,
                now=now, requestContext=requestContext
            )

            queries_and_generators.append((n, query, gen))

        results = []

        for n, query, func in queries_and_generators:
            time_info, values = func()

            results.append({
                'pathExpression': query.pattern,
                'path': n.path,
                'name': n.path,
                'time_info': time_info,
                'values': values,
            })

        return results
