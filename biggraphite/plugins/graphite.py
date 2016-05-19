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

from graphite import intervals
from graphite import node


from biggraphite import accessor

_CONFIG_NAME = "biggraphite"


class Error(Exception):
    """Base class for all exceptions from this module."""


class ConfigError(Error):
    """Configuration problems."""


def _round_down(rounded, divider):
    return int(rounded) // divider * divider


def _round_up(rounded, divider):
    return int(rounded + divider - 1) // divider * divider


class Reader(object):
    """As per the Graphite API, fetches points for and metadata for a given metric."""

    __slots__ = ("_accessor", "_metric", "_metadata", )

    def __init__(self, accessor, metric):
        """Create a new reader."""
        self._accessor = accessor
        self._metric = metric
        self._metadata = None

    def __get_time_info(self, start_time, end_time, now):
        """Constrain the provided range in an aligned interval within retention."""
        # TODO: We do not support downsampling yet.
        if self._metadata and self._metadata.carbon_retentions:
            step, retention = self._metadata.carbon_retentions[0]
        else:
            step, retention = 1, 60

        now = _round_up(now, step)

        oldest_timestamp = now - retention * step
        start_time = max(start_time, oldest_timestamp)
        start_time = _round_down(start_time, step)

        end_time = min(now, end_time)
        end_time = _round_up(end_time, step)

        if end_time < start_time:
            end_time = start_time
        return start_time, end_time, step

    def __refresh_metadata(self):
        if self._metadata is None:
            self._metadata = self._accessor.get_metric(self._metric)

    def fetch(self, start_time, end_time, now=None):
        """Fetch point for a given interval as per the Graphite API.

        Args:
          start_time: Timestamp to fetch points from, will constrained by retention policy.
          end_time: Timestamp to fetch points until, will constrained by retention policy.
          now: Current timestamp as a float, defaults to time.time(), for tests.

        Returns:
          A tuple made of (rounded start time, rounded end time, step as per retention), points
          Points is a list for which missing points are set to None.
        """
        self.__refresh_metadata()
        if now is None:
            now = time.time()

        # TODO: We do not support downsampling yet.
        start_time, end_time, step = self.__get_time_info(start_time, end_time, now)
        ts_and_points = self._accessor.fetch_points(self._metric, start_time, end_time, step,
                                                    self._metadata.carbon_aggregate_points)
        points_num = (end_time - start_time) // step
        # TODO: Consider wrapping an array (using NaN for None) for speed&memory efficiency
        points = [None] * points_num
        for ts, point in ts_and_points:
            index = int(ts - start_time) // step
            points[index] = point
        return (start_time, end_time, step), points

    def get_intervals(self, now=None):
        """Fetch information on the retention policy, as per the Graphite API.

        Args:
          now: Current timestamp as a float, defaults to time.time(), for tests.

        Returns:
          A list of interval.Intervals for which we have data.
        """
        self.__refresh_metadata()
        if now is None:
            now = time.time()
        # Call __get_time_info with the widest conceivable range will make it be
        # shortened to the widest range available according to retention policy.
        # TODO: Update when we support multiple retention policies.
        start, end, unused_step = self.__get_time_info(0, now, now)
        return intervals.IntervalSet([intervals.Interval(start, end)])


class Finder(object):
    """Fake to test the Reader."""

    def __init__(self, directories=None, django_like_settings=None):
        """Build a new finder.

        Args:
          directories: Ignored (here only for compatibility)
          django_like_settings: An object to use instead of django.conf.settings
            to inject configuration variables in unit tests.
        """
        if not django_like_settings:
            # TODO: Support graphite-API like config
            from django.conf import settings as django_settings
            django_like_settings = django_settings

        keyspace = getattr(django_like_settings, "BG_KEYSPACE", None)
        contact_points = getattr(django_like_settings, "BG_CONTACT_POINTS", None)
        port = getattr(django_like_settings, "BG_PORT", None)

        if not keyspace:
            raise ConfigError("BG_KEYSPACE is mandatory")
        if not contact_points:
            raise ConfigError("BG_CONTACT_POINTS are mandatory")
        # port is optional

        self._accessor = accessor.Accessor(keyspace, contact_points, port)
        self._accessor.connect()

    def find_nodes(self, query):
        """Fake to allow testing the reader."""
        pattern = query.pattern
        metric_path = "test_metric"
        if pattern == "*" or pattern == metric_path:
            reader = Reader(self._accessor, metric_path)
            yield node.LeafNode(metric_path, reader)
