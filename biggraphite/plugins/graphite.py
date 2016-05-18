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


def _round_down(rounded, devider):
    return int(rounded) // devider * devider


def _round_up(rounded, devider):
    return int(rounded + devider - 1) // devider * devider


class Reader(object):

    __slots__ = ("_accessor", "_metric", "_metadata", )

    def __init__(self, accessor, metric):
        self._accessor = accessor
        self._metric = metric
        self._metadata = None

    def __get_time_info(self, start_time, end_time):
        """Constrain the provided range in an aligned interval within retention."""
        # TODO: We do not support downsampling yet.
        if self._metadata and self._metadata.carbon_retentions:
            step, retention = self._metadata.carbon_retentions[0]
        else:
            step, retention = 1, 60

        oldest_timestamp = time.time() - retention
        start_time = max(start_time, oldest_timestamp)
        start_time = _round_down(start_time, step)

        end_time = min(time.time(), end_time)
        end_time = _round_up(end_time, step)

        if end_time < start_time:
            end_time = start_time
        return start_time, end_time, step

    def __refresh_metadata(self):
        if self._metadata is None:
            self._metadata = self._accessor.get_metric(self._metric)

    def fetch(self, start_time, end_time):
        self.__refresh_metadata()
        # TODO: We do not support downsampling yet.
        start_time, end_time, step = self.__get_time_info(start_time, end_time)
        ts_and_points = self._accessor.fetch_points(self._metric, start_time, end_time, step,
                                                    self._metadata.carbon_aggregate_points)
        points_num = (end_time - start_time) // step
        # TODO: Consider wrapping an array (using NaN for None) for speed&memory efficiency
        points = [None] * points_num
        for ts, point in ts_and_points:
            index = int(ts - start_time) // step
            points[index] = point
        return (start_time, end_time, step), points

    def get_intervals(self):
        self.__refresh_metadata()
        start, end, unused_step = self.__get_time_info(0, time.time())
        return intervals.IntervalSet([intervals.Interval(start, end)])


class Finder(object):

    def __init__(self, directories=None, django_like_settings=None):
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
        if pattern == "*" or pattern.startswith(metric_path):
            reader = Reader(self._accessor, metric_path)
            yield node.LeafNode(metric_path, reader)
