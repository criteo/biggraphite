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

from biggraphite import accessor as bg_accessor

import array
import math

_NaN = float("NaN")


class Downsampler(object):
    """Downsampler using MetricAggregates to produce aggregates."""

    CAPACITY = 20

    slots = (
        "_capacity",
        "_names_to_aggregates"
    )

    def __init__(self, capacity=CAPACITY):
        """Default constructor."""
        self._capacity = capacity
        self._names_to_aggregates = {}

    def feed(self, metric, datapoints):
        """Feed the downsampler and produce points.

        Arg:
          metric: Metric
          datapoints: iterable of (timestamp, value)
        Returns:
          Iterable of (timestamp, value, count, precision).
        """
        # Ensure we have the required aggregation mechanism.
        if metric.name not in self._names_to_aggregates:
            metric_aggregates = MetricAggregates(metric.metadata, self._capacity)
            self._names_to_aggregates[metric.name] = metric_aggregates

        # Sort points by increasing timestamp, because put expects them in order.
        return self._names_to_aggregates[metric.name].update(metric.metadata, sorted(datapoints))


class MetricAggregates(object):
    """Perform aggregation on metric data points."""

    DEFAULT_RAW_CAPACITY = 10

    __slots__ = (
        "_raw_capacity",   # TODO: remove (use stage in methods)
        "_timestamps",
        "_values",
        "_counts",
    )

    def __init__(self, metric_metadata, raw_capacity=DEFAULT_RAW_CAPACITY):
        """"Initialize a MetricAggregates object.

        Args:
          metric_metadata: MetricMetadata object.
          precision: precision of the raw buffer in seconds.
          capacity: number of slots in the raw buffer.
        """
        # TODO: use stage in constructor
        stages = len(metric_metadata.retention.stages)

        # _raw_capacity: length of the raw buffer
        self._raw_capacity = raw_capacity

        # _timestamps: array of integers:
        #   - raw timestamp at index 0
        #   - stage timestamp from index 1
        # TODO: make sure that each stage timestamp is equal to:
        # (raw_timestamp - (raw_capacity - 1) * raw_precision) % stage_precision
        self._timestamps = array.array("i", [-1] * (1 + stages))

        # _values: array of doubles:
        #   - raw buffer values from index 0 with length raw_capacity
        #   - stage values from index raw_capacity
        self._values = array.array("d", [_NaN] * (raw_capacity + stages))

        # _counts: array of integer: stage counts from index 0
        self._counts = array.array("i", [0] * stages)

    def _update_raw(self, datapoints, precision):
        """"Put raw data points in raw buffer and pop expired raw data points.

        Args:
          datapoints: iterable of (timestamp, value).
          precision: precision of the raw buffer in seconds.

        Returns:
          The list of (timestamp, value) expired from the raw buffer.
        """
        if self._raw_timestamp == -1:
            # Raw buffer is empty.
            if not datapoints:
                # No update => nothing to expire.
                return []
            # Otherwise, update raw timestamp to first point.
            first_datapoint = datapoints[0]
            first_timestamp = first_datapoint[0]
            self._raw_timestamp = first_timestamp

        expired = []
        for timestamp, value in datapoints:
            last_update_epoch = self._raw_timestamp // precision
            point_epoch = timestamp // precision
            if point_epoch > last_update_epoch:
                # Point is more recent than most recent raw point => expire.
                # If we add N points, we have to expire N points.
                # Here, N = point_epoch - last_update_epoch.
                expired_count = point_epoch - last_update_epoch

                # However, N can be larger than the raw buffer capacity.
                # But we only need to expire as many points as the raw buffer capacity.
                expired_count = min(expired_count, self._raw_capacity)

                # The first point to expire is the oldest.
                start_epoch = last_update_epoch - (self._raw_capacity - 1)
                end_epoch = start_epoch + expired_count
                for epoch in xrange(start_epoch, end_epoch):
                    index = epoch % self._raw_capacity
                    if not math.isnan(self._values[index]):
                        expired.append((epoch * precision, self._values[index]))
                    self._values[index] = _NaN
                self._raw_timestamp = bg_accessor.round_down(timestamp, precision)
                self._values[point_epoch % self._raw_capacity] = value
            elif point_epoch > last_update_epoch - self._raw_capacity:
                # Point fits in the buffer => replace value in the raw buffer.
                self._values[point_epoch % self._raw_capacity] = value
        return expired

    def _update_stage(self, metric_metadata, stage, points):
        """Compute aggregated value for a stage and store it.

        The points have to be sorted by increasing timestamps.

        Args:
          metric_metadata: MetricMetadata object.
          stage: index of stage to update with raw points.
          points: raw points to be added into the current stage aggregate.

        Returns:
          Iterable of (timestamp, value, count, stage).
        """
        stages = metric_metadata.retention.stages
        stage_object = stages[stage]
        precision = stage_object.precision
        aggregator = metric_metadata.aggregator

        current_timestamp = self._get_stage_timestamp(stage)
        current_value = self._get_stage_value(stage)
        current_count = self._get_stage_count(stage)
        if current_timestamp == -1 and not points:
            return []

        if current_timestamp == -1:
            # Raw buffer is empty  => take first point timestamp.
            first_point = points[0]
            first_epoch = first_point[0] // precision
            first_timestamp = first_epoch * precision
            current_timestamp = first_timestamp

        expired = [(current_timestamp, current_value, current_count, stage_object)]

        for timestamp, value in points:
            epoch = timestamp // precision
            current_point = expired[-1]
            current_epoch = current_point[0] // precision
            if current_epoch == epoch:
                # Point is in current epoch => aggregate:
                #   1. Get current value and and current count.
                #   2. Compute aggregated value with new value.
                #   3. Update stage information.
                # The last point in expired now contains up-to-date information.
                current_value = current_point[1]
                current_count = current_point[2]
                aggregated_value = aggregator.merge(current_value, current_count, value)
                expired[-1] = (epoch * precision, aggregated_value, current_count + 1, stage_object)
            elif current_epoch < epoch:
                # Point is in new epoch => add new epoch.
                expired.append((epoch * precision, value, 1, stage_object))
        if expired:
            current_point = expired[-1]
            self._set_stage_timestamp(stage, current_point[0])
            self._set_stage_value(stage, current_point[1])
            self._set_stage_count(stage, current_point[2])

        return expired

    def update(self, metric_metadata, datapoints):
        """"Compute aggregated values and store them.

        The points have to be sorted by increasing timestamps.

        Args:
          metric_metadata: MetricMetadata object
          datapoints: iterable of (timestamp, value).

        Returns:
          The list of expired (timestamp, value, count, stage) for all stages.
        """
        stages = metric_metadata.retention.stages
        raw_precision = stages[0].precision
        expired_raw = self._update_raw(datapoints, raw_precision)
        expired = []
        for stage in xrange(len(metric_metadata.retention.stages)):
            expired.extend(self._update_stage(metric_metadata, stage, expired_raw))
        return expired

    @property
    def _raw_timestamp(self):
        """Get timestamp of raw buffer."""
        return self._timestamps[0]

    @_raw_timestamp.setter
    def _raw_timestamp(self, value):
        """Set timestamp of raw buffer."""
        self._timestamps[0] = value

    def _get_stage_timestamp(self, stage):
        """Get timestamp of stage in buffer.

        Raw timestamp is at index 0.
        Stage timestamps start at index 1.

        Args:
          stage: stage whose timestamp to get.

        Returns:
          Timestamp of stage, in seconds.
        """
        return self._timestamps[1 + stage]

    def _set_stage_timestamp(self, stage, timestamp):
        """Set timestamp of stage in buffer.

        Raw timestamp is at index 0.
        Stage timestamps start at index 1.

        Args:
          stage: stage whose timestamp to set.
          timestamp: timestamp to set stage to.
        """
        self._timestamps[1 + stage] = timestamp

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
