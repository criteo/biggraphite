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

"""Downsampling helpers for drivers that do not implement it server-side."""
from __future__ import absolute_import
from __future__ import print_function

import array
import math

_NaN = float("NaN")


class MetricDownsampler(object):
    """Metric-bound downsampler using MetricAggregates to produce aggregates."""

    slots = (
        "_metric_aggregates"
    )

    def __init__(self, metric_metadata, raw_capacity):
        """Initialize a new MetricDownsampler.

        Args:
          metric_metadata: MetricMetadata object.
          raw_capacity: capacity of the raw buffer.
        """
        self._metric_aggregates = MetricAggregates(metric_metadata, raw_capacity)

    def feed(self, metric_metadata, datapoints):
        """Feed the downsampler and produce points.

        Points older than the latest ingested point may be ignored.

        Arg:
          metric_metadata: MetricMetadata object.
          datapoints: iterable of (timestamp, value).
        Returns:
          Iterable of (timestamp, value, count, precision).
        """
        # Sort points by increasing timestamp, because put expects them in order.
        return self._metric_aggregates.update(metric_metadata, sorted(datapoints))


class Downsampler(object):
    """Downsampler using MetricAggregates to produce aggregates."""

    CAPACITY = 20

    slots = (
        "__capacity"
    )

    def __init__(self, capacity=CAPACITY):
        """Default constructor."""
        self.__capacity = capacity
        self.__metrics = {}

    def feed(self, metric, datapoints):
        """Feed the downsampler and produce points.

        Arg:
          metric: Metric
          datapoints: iterable of (timestamp as sec, value as float)
        Returns:
          Iterable of (timestamp, value, count, precision).
        """
        # Ensure we have the required aggregation mechanism.
        if metric.name not in self.__metrics:
            self.__metrics[metric.name] = MetricDownsampler(metric.metadata, self.__capacity)

        return self.__metrics[metric.name].feed(metric.metadata, datapoints)


class MetricAggregates(object):
    """Perform aggregation on metric data points."""

    RAW_CAPACITY = 10

    __slots__ = (
        "_raw_timestamp",
        "_raw_capacity",   # TODO: remove (use stage in methods)
        "_timestamps",
        "_values",
        "_counts",
    )

    def __init__(self, metric_metadata, raw_capacity=RAW_CAPACITY):
        """"Initialize a MetricAggregates object.

        Args:
          metric_metadata: MetricMetadata object.
          precision: precision of the raw buffer in seconds.
          capacity: number of slots in the raw buffer.
        """
        # TODO: use stage in constructor
        stages = len(metric_metadata.retention.stages)
        self._raw_timestamp = -1
        self._raw_capacity = raw_capacity
        self._timestamps = array.array("i", [-1] * stages)
        self._values = array.array("d", [_NaN] * (raw_capacity + stages))
        self._counts = array.array("i", [0] * stages)

    def _update_raw(self, datapoints, precision):
        """"Put raw data points in raw buffer and pop expired raw data points.

        Args:
          datapoints: iterable of (timestamp, value).
          precision: precision of the raw buffer in seconds.

        Returns:
          The list of (timestamp, value) expired from the raw buffer.
        """
        res = []
        for timestamp, value in datapoints:
            if self._raw_timestamp == -1:
                self._raw_timestamp = timestamp
            current_epoch = self._raw_timestamp // precision
            point_epoch = timestamp // precision
            if point_epoch > current_epoch:
                # Point is more recent than most recent raw point => expire.
                start_epoch = current_epoch - (self._raw_capacity - 1)
                end_epoch = point_epoch - (self._raw_capacity - 1)
                for epoch in xrange(start_epoch, end_epoch):
                    index = epoch % self._raw_capacity
                    if not math.isnan(self._values[index]):
                        res.append((epoch * precision, self._values[index]))
                    self._values[index] = _NaN
                self._raw_timestamp = point_epoch * precision
                self._values[point_epoch % self._raw_capacity] = value
            elif point_epoch > current_epoch - self._raw_capacity:
                self._values[point_epoch % self._raw_capacity] = value
        return res

    def _update_stage(self, stage, precision, aggregator, points):
        """Compute aggregated value for a stage and store it.

        The points have to be sorted by increasing timestamps.

        Args:
          stage: stage to update with raw points.
          precision: stage precision, in seconds.
          aggregator: metric aggregator function.
          points: raw points to be added into the current stage aggregate.

        Returns:
          Iterable of (timestamp, value, count, precision).
        """
        current_timestamp = self._get_stage_timestamp(stage)
        current_value = self._get_stage_value(stage)
        current_count = self._get_stage_count(stage)
        if current_timestamp == -1 and not points:
            return []
        res = [(current_timestamp, current_value, current_count, precision)]
        for timestamp, value in points:
            epoch = timestamp // precision
            current_point = res[-1]
            current_epoch = current_point[0] // precision
            if current_epoch == -1:
                # No prior stage information => take first point.
                res[0] = (epoch * precision, value, 1, precision)
            elif current_epoch == epoch:
                # Point is in current epoch => aggregate:
                #   1. Get current value and and current count.
                #   2. Compute aggregated value with new value.
                #   3. Update stage information.
                # The last point in res now contains up-to-date information.
                current_value = current_point[1]
                current_count = current_point[2]
                aggregated_value = aggregator.merge(current_value, current_count, value)
                res[-1] = (epoch * precision, aggregated_value, current_count + 1, precision)
            elif current_epoch < epoch:
                # Point is in new epoch => add new epoch.
                res.append((epoch * precision, value, 1, precision))
        if res:
            current_point = res[-1]
            self._set_stage_timestamp(stage, current_point[0])
            self._set_stage_value(stage, current_point[1])
            self._set_stage_count(stage, current_point[2])

        return res

    def update(self, metric_metadata, datapoints):
        """"Compute aggregated values and store them.

        The points have to be sorted by increasing timestamps.

        Args:
          metric_metadata: MetricMetadata object
          datapoints: iterable of (timestamp, value).

        Returns:
          # TODO: change
          The list of expired (timestamp, value) from the raw buffer.
        """
        stages = metric_metadata.retention.stages
        aggregator = metric_metadata.aggregator
        raw_precision = stages[0].precision
        expired_raw = self._update_raw(datapoints, raw_precision)
        expired = []
        for stage in xrange(len(stages)):
            stage_precision = stages[stage].precision
            expired.extend(self._update_stage(stage, stage_precision, aggregator, expired_raw))
        return expired

    def _get_stage_timestamp(self, stage):
        """Get timestamp of stage in buffer.

        Args:
          stage: stage whose timestamp to get.

        Returns:
          Timestamp of stage, in seconds.
        """
        return self._timestamps[stage]

    def _set_stage_timestamp(self, stage, timestamp):
        """Set timestamp of stage in buffer.

        Args:
          stage: stage whose timestamp to set.
          timestamp: timestamp to set stage to.
        """
        self._timestamps[stage] = timestamp

    def _get_stage_value(self, stage):
        """Get value of stage in buffer.

        Args:
          stage: stage whose value to get.

        Returns:
          Value of stage.
        """
        return self._values[self._raw_capacity + stage]

    def _set_stage_value(self, stage, value):
        """Set value of stage in buffer.

        Args:
          stage: stage whose value to set.
          value: value to set stage to.
        """
        self._values[self._raw_capacity + stage] = value

    def _get_stage_count(self, stage):
        """Get count of stage in buffer.

        Args:
          stage: stage whose count to get.

        Returns:
          Count of stage.
        """
        return self._counts[stage]

    def _set_stage_count(self, stage, count):
        """Set count of stage in buffer.

        Args:
          stage: stage whose count to set.
          count: count to set stage to.
        """
        self._counts[stage] = count
