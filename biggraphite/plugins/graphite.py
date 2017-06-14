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
"""Adapter between BigGraphite and Graphite."""
from __future__ import absolute_import  # Otherwise graphite is this module.

import time
import threading

from graphite import intervals
from graphite import node
from graphite import readers
from graphite import finders
from graphite import carbonlink
from graphite.logger import log
from graphite.render import hashing

from biggraphite import accessor as bg_accessor
from biggraphite import accessor_cache as bg_accessor_cache
from biggraphite import glob_utils
from biggraphite import graphite_utils


_CONFIG_NAME = "biggraphite"

try:
    BaseReader = readers.utils.BaseReaders
except AttributeError:
    BaseReader = object
try:
    BaseFinder = finders.utils.BaseFinder
except AttributeError:
    BaseFinder = object


class Error(Exception):
    """Base class for all exceptions from this module."""


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
            if stage.stage0 and self._carbonlink:
                cached_datapoints = self._carbonlink.query(self._metric_name)
        except Exception:
            log.exception("Failed CarbonLink query '%s'" % self._metric_name)
        return cached_datapoints

    def __get_time_info(self, start_time, end_time, now, shift=False):
        """Constrain the provided range in an aligned interval within retention."""
        self.__refresh_metric()
        if self._metric and self._metric.retention:
            retention = self._metric.retention
        else:
            retention = bg_accessor.Retention.from_string("60*60s")

        return retention.align_time_window(
            start_time, end_time, now, shift=shift)

    def __refresh_metric(self):
        """Set self._metric."""
        if self._metric is None:
            self._metric = self._metadata_cache.get_metric(self._metric_name)

    def _merge_cached_points(self, stage, start_step, points, cached_points):
        """Merge points from carbonlink with points from database."""
        if not cached_points:
            return points

        for (timestamp, value) in cached_points:
            step = int(timestamp - (timestamp % stage.precision)) / stage.precision

            index = step - start_step
            if index < 0 or index > len(points):
                continue
            points[index] = value

        return points

    def fetch(self, start_time, end_time, now=None):
        """Fetch point for a given interval as per the Graphite API.

        Args:
          start_time: Timestamp to fetch points from, will constrained by retention policy.
          end_time: Timestamp to fetch points until, will constrained by retention policy.
          now: Current timestamp as a float, defaults to time.time(), for tests.

        Returns:
          A tuple made of (rounded start time, rounded end time, stage precision), points
          Points is a list for which missing points are set to None.
        """
        self.__refresh_metric()

        fetch_start = time.time()
        log.rendering('fetch(%s, %d, %d) - start' % (
            self._metric_name, start_time, end_time))

        self.__refresh_metric()
        if now is None:
            now = time.time()

        start_time, end_time, stage = self.__get_time_info(start_time, end_time, now)
        start_step = stage.step(start_time)
        points_num = stage.step(end_time) - start_step

        if not self._metric:
            # The metric doesn't exist, let's fail gracefully.
            ts_and_points = []
        else:
            # This returns a generator which we can iterate on later.
            ts_and_points = self._accessor.fetch_points(
                self._metric, start_time, end_time, stage)

        cached_datapoints = self.__get_cached_datapoints(stage)

        def read_points():
            read_start = time.time()
            # TODO: Consider wrapping an array (using NaN for None) for
            # speed&memory efficiency
            points = [None] * points_num
            for ts, point in ts_and_points:
                index = stage.step(ts) - start_step
                points[index] = point

            if cached_datapoints:
                points = self._merge_cached_points(
                    stage, start_step, points, cached_datapoints)

            now = time.time()
            log.rendering(
                'fetch(%s, %d, %d) - %d points - read: %f secs - total: %f secs' % (
                    self._metric_name, start_time, end_time, len(points),
                    now - read_start, now - fetch_start))
            return (start_time, end_time, stage.precision), points

        log.rendering('fetch(%s, %d, %d) - started' % (
            self._metric_name, start_time, end_time))
        return readers.FetchInProgress(read_points)

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
        start, end, unused_stage = self.__get_time_info(0, now, now, shift=True)
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
                accessor = graphite_utils.accessor_from_settings(django_settings)
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
                if callable(carbonlink.CarbonLink):
                    self._carbonlink = carbonlink.CarbonLink()
                else:
                    self._carbonlink = carbonlink.CarbonLink
        return self._carbonlink

    def cache(self):
        """Return a metadata cache."""
        with self._lock:
            if not self._cache:
                # TODO: Allow to use Django's cache.
                from django.conf import settings as django_settings
                cache = graphite_utils.cache_from_settings(self.accessor(), django_settings)
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

    def find_nodes(self, query):
        """Find nodes matching a query."""
        # TODO: we should probably consider query.startTime and query.endTime
        #  to filter out metrics that had no points in this interval.

        leaves_only = hasattr(query, 'leaves_only') and query.leaves_only
        cache_key = "find_nodes:%s" % (hashing.compactHash(query.pattern))
        cached = self.django_cache().get(cache_key)
        if cached:
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
                self.accessor(), self.cache(), self.carbonlink(), metric_name)
            yield node.LeafNode(metric_name, reader)

        for directory in directories:
            yield node.BranchNode(directory)
