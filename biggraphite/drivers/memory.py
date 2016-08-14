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
"""Simple memory-based accessor for tests and development."""
from __future__ import absolute_import
from __future__ import print_function

import collections
import fnmatch
import re

import sortedcontainers

from biggraphite import accessor as bg_accessor
from biggraphite.drivers import _downsampling


class _MemoryAccessor(bg_accessor.Accessor):
    """A memory acessor that doubles as a memory MetadataCache."""

    def __init__(self):
        """Create a new MemoryAccessor."""
        super(_MemoryAccessor, self).__init__("memory")
        self._metric_to_points = collections.defaultdict(
            sortedcontainers.SortedDict)
        self._metric_to_metadata = {}
        self._directory_names = sortedcontainers.SortedSet()
        self.__downsampler = _downsampling.Downsampler()

    @property
    def _metric_names(self):
        return self._metric_to_metadata.keys()

    def connect(self, *args, **kwargs):
        """See the real Accessor for a description."""
        super(_MemoryAccessor, self).connect(*args, **kwargs)
        self.is_connected = True

    def shutdown(self, *args, **kwargs):
        """See the real Accessor for a description."""
        super(_MemoryAccessor, self).shutdown(*args, **kwargs)
        self.is_connected = False

    def insert_points_async(self, metric, datapoints, on_done=None):
        """See the real Accessor for a description."""
        super(_MemoryAccessor, self).insert_points_async(
            metric, datapoints, on_done)
        assert metric.name in self._metric_to_metadata
        points = self._metric_to_points[metric.name]
        # TODO(c.chary): uncomment that when the downsampler has been
        #   fixed. Currently it waits a few iteration before emitting points.
        #   it will also need additional fixing to read aggregated metrics
        #   while storing downsampled metrics.
        # datapoints = self.__downsampler.feed(metric, datapoints)
        for datapoint in datapoints:
            timestamp, value = datapoint
            points[timestamp] = value
        if on_done:
            on_done(None)

    def drop_all_metrics(self, *args, **kwargs):
        """See the real Accessor for a description."""
        super(_MemoryAccessor, self).drop_all_metrics(*args, **kwargs)
        self._metric_to_points.clear()
        self._metric_to_metadata.clear()
        self._directory_names.clear()

    def create_metric(self, metric):
        """See the real Accessor for a description."""
        super(_MemoryAccessor, self).create_metric(metric)
        self._metric_to_metadata[metric.name] = metric.metadata
        parts = metric.name.split(".")[:-1]
        path = []
        for part in parts:
            path.append(part)
            self._directory_names.add(".".join(path))

    @staticmethod
    def __glob_names(names, glob):
        res = []
        dots_count = glob.count(".")
        glob_re = re.compile(fnmatch.translate(glob))
        for name in names:
            # "*" can match dots for fnmatch
            if name.count(".") == dots_count and glob_re.match(name):
                res.append(name)
        return res

    def glob_metric_names(self, glob):
        """See the real Accessor for a description."""
        super(_MemoryAccessor, self).glob_metric_names(glob)
        return self.__glob_names(self._metric_names, glob)

    def glob_directory_names(self, glob):
        """See the real Accessor for a description."""
        super(_MemoryAccessor, self).glob_directory_names(glob)
        return self.__glob_names(self._directory_names, glob)

    def get_metric(self, metric_name):
        """See the real Accessor for a description."""
        super(_MemoryAccessor, self).get_metric(metric_name)
        metadata = self._metric_to_metadata.get(metric_name)
        if metadata:
            return bg_accessor.Metric(metric_name, metadata)
        else:
            return None

    def fetch_points(self, metric, time_start, time_end, stage):
        """See the real Accessor for a description."""
        super(_MemoryAccessor, self).fetch_points(
            metric, time_start, time_end, stage)
        points = self._metric_to_points[metric.name]
        rows = []
        for ts in points.irange(time_start, time_end):
            # A row is time_base_ms, time_offset_ms, value, count
            row = (ts * 1000.0, 0, float(points[ts]), 1)
            rows.append(row)
        query_results = [(True, rows)]

        time_start_ms = int(time_start) * 1000
        time_end_ms = int(time_end) * 1000
        return bg_accessor.PointGrouper(
            metric, time_start_ms, time_end_ms, stage, query_results)


def build(*args, **kwargs):
    """Return a bg_accessor.Accessor using memory."""
    return _MemoryAccessor(*args, **kwargs)
