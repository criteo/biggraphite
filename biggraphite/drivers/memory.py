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
import uuid

import sortedcontainers

from biggraphite import accessor as bg_accessor
from biggraphite import glob_utils as bg_glob
from biggraphite.drivers import _downsampling

OPTIONS = {}


class _MemoryAccessor(bg_accessor.Accessor):
    """A memory acessor that doubles as a memory MetadataCache."""

    Row = collections.namedtuple(
        'Row', ['time_start_ms', 'offset', 'value', 'count'])

    _UUID_NAMESPACE = uuid.UUID('{00000000-1111-2222-3333-444444444444}')

    def __init__(self):
        """Create a new MemoryAccessor."""
        super(_MemoryAccessor, self).__init__("memory")
        self._metric_to_points = collections.defaultdict(
            sortedcontainers.SortedDict)
        self._name_to_metric = {}
        self._directory_names = sortedcontainers.SortedSet()
        self.__downsampler = _downsampling.Downsampler()

    @staticmethod
    def _components_from_name(metric_name):
        res = metric_name.split(".")
        return filter(None, res)

    @property
    def _metric_names(self):
        return self._name_to_metric.keys()

    def connect(self, *args, **kwargs):
        """See the real Accessor for a description."""
        super(_MemoryAccessor, self).connect(*args, **kwargs)
        self.is_connected = True

    def shutdown(self, *args, **kwargs):
        """See the real Accessor for a description."""
        super(_MemoryAccessor, self).shutdown(*args, **kwargs)
        self.is_connected = False

    def repair(self, *args, **kwargs):
        """See the real Accessor for a description."""
        pass

    def insert_points_async(self, metric, datapoints, on_done=None):
        """See the real Accessor for a description."""
        super(_MemoryAccessor, self).insert_points_async(
            metric, datapoints, on_done)
        assert metric.name in self._name_to_metric
        datapoints = self.__downsampler.feed(metric, datapoints)
        for datapoint in datapoints:
            timestamp, value, count, stage = datapoint
            points = self._metric_to_points[(metric.name, stage)]
            points[timestamp] = (value, count)
        if on_done:
            on_done(None)

    def insert_downsampled_points_async(self, metric, downsampled, on_done=None):
        """See the real Accessor for a description."""
        datapoints = [(ts, value) for ts, value, count, stage in downsampled]
        return self.insert_points_async(metric, datapoints, on_done)

    def drop_all_metrics(self, *args, **kwargs):
        """See the real Accessor for a description."""
        super(_MemoryAccessor, self).drop_all_metrics(*args, **kwargs)
        self._metric_to_points.clear()
        self._name_to_metric.clear()
        self._directory_names.clear()

    def make_metric(self, name, metadata):
        """See bg_accessor.Accessor."""
        # Cleanup name (avoid double dots)
        name = ".".join(self._components_from_name(name))
        uid = uuid.uuid5(self._UUID_NAMESPACE, name)
        return bg_accessor.Metric(name, uid, metadata)

    def create_metric(self, metric):
        """See the real Accessor for a description."""
        super(_MemoryAccessor, self).create_metric(metric)
        self._name_to_metric[metric.name] = metric
        components = self._components_from_name(metric.name)
        path = []
        for part in components[:-1]:
            path.append(part)
            self._directory_names.add(".".join(path))

    @staticmethod
    def __glob_names(names, glob):
        results = bg_glob.glob(names, glob)
        results.sort()
        return results

    def glob_metric_names(self, glob):
        """See the real Accessor for a description."""
        super(_MemoryAccessor, self).glob_metric_names(glob)
        return self.__glob_names(self._metric_names, glob)

    def glob_directory_names(self, glob):
        """See the real Accessor for a description."""
        super(_MemoryAccessor, self).glob_directory_names(glob)
        return self.__glob_names(self._directory_names, glob)

    def has_metric(self, metric_name):
        """See bg_accessor.Accessor."""
        super(_MemoryAccessor, self).has_metric(metric_name)
        return self.get_metric(metric_name) is not None

    def get_metric(self, metric_name):
        """See the real Accessor for a description."""
        super(_MemoryAccessor, self).get_metric(metric_name)
        metric_name = ".".join(self._components_from_name(metric_name))
        return self._name_to_metric.get(metric_name)

    def fetch_points(self, metric, time_start, time_end, stage):
        """See the real Accessor for a description."""
        super(_MemoryAccessor, self).fetch_points(
            metric, time_start, time_end, stage)
        points = self._metric_to_points[(metric.name, stage)]
        rows = []
        for ts in points.irange(time_start, time_end):
            # A row is time_base_ms, time_offset_ms, value, count
            row = self.Row(ts * 1000.0, 0, float(points[ts][0]), points[ts][1])
            rows.append(row)

        query_results = [(True, rows)]
        time_start_ms = int(time_start) * 1000
        time_end_ms = int(time_end) * 1000
        return bg_accessor.PointGrouper(
            metric, time_start_ms, time_end_ms, stage, query_results)


def build(*args, **kwargs):
    """Return a bg_accessor.Accessor using memory."""
    return _MemoryAccessor(*args, **kwargs)
