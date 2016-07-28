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

    def feed(self, metric, points):
        """Feed the downsampler and produce points.

        Arg:
          metric: Metric
          points: iterable of (timestamp, value)
        Returns:
          Iterable of (timestamp, value, count, precision).
        """
        # Ensure we have the required aggregation mechanism.
        if metric.name not in self._names_to_aggregates:
            metric_aggregates = MetricAggregates(metric.metadata, self._capacity)
            self._names_to_aggregates[metric.name] = metric_aggregates

        # Sort points by increasing timestamp, because put expects them in order.
        return self._names_to_aggregates[metric.name].update(metric.metadata, sorted(points))


class MetricAggregates(object):
    """Perform aggregation on metric points."""

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

    def _get_expired_raw_points(self, points, retention):
        """"Put raw data points in raw buffer and return expired raw points.

        Args:
          points: iterable of (timestamp, value).
          rentention: Retention object.

        Returns:
          The list of (timestamp, value) of expired raw points:
        """
        if self._raw_timestamp == -1:
            # Raw buffer is empty.
            if not points:
                # No update => nothing to expire.
                return []
            # Otherwise, update raw timestamp to first point.
            first_datapoint = points[0]
            first_timestamp = first_datapoint[0]
            self._raw_timestamp = first_timestamp

        stage_0 = retention[0]
        expired = []
        for timestamp, value in points:
            last_update_step = stage_0.step(self._raw_timestamp)
            point_step = stage_0.step(timestamp)
            if point_step > last_update_step:
                # Point is more recent than most recent raw point => expire.
                # If we add N points, we have to expire N points.
                # Here, N = point_step - last_update_step.
                expired_count = point_step - last_update_step

                # However, N can be larger than the raw buffer capacity.
                # But we only need to expire as many points as the raw buffer capacity.
                expired_count = min(expired_count, self._raw_capacity)

                # The first point to expire is the oldest.
                start_step = last_update_step - (self._raw_capacity - 1)
                end_step = start_step + expired_count
                for step in xrange(start_step, end_step):
                    index = step % self._raw_capacity
                    if not math.isnan(self._values[index]):
                        expired.append((step * stage_0.precision, self._values[index]))
                    self._values[index] = _NaN
                self._raw_timestamp = stage_0.round_down(timestamp)

                self._values[point_step % self._raw_capacity] = value
            elif point_step > last_update_step - self._raw_capacity:
                # Point fits in the buffer => replace value in the raw buffer.
                self._values[point_step % self._raw_capacity] = value

        return expired

    def _get_non_expired_raw_points(self, retention):
        """"Get current (non expired) raw points.

        Args:
          retention: Retention object.

        Returns:
          The list of (timestamp, value) of non-expired raw points.
        """
        if self._raw_timestamp == -1:
            # Raw buffer is empty.
            return []

        result = []
        stage_0 = retention[0]
        start_step = stage_0.step(self._raw_timestamp) - (self._raw_capacity - 1)
        end_step = start_step + self._raw_capacity
        for step in xrange(start_step, end_step):
            index = step % self._raw_capacity
            if not math.isnan(self._values[index]):
                result.append((step * stage_0.precision, self._values[index]))

        return result

    def _update_raw(self, retention, points):
        """"Put raw points in raw buffer and return all raw points.

        Args:
          retention: Retention object.
          points: iterable of (timestamp, value).

        Returns:
          A tuple of (expired, non expired) raw points.
          Each element of the tuple is list of (timestamp, value) of raw points.
        """
        expired = self._get_expired_raw_points(points, retention)
        non_expired = self._get_non_expired_raw_points(retention)
        return (expired, non_expired)

    def _coalesce(self, stage, metric_metadata, result, points):
        """"Coalesce raw points into result.

        Args:
          stage: Stage object.
          metric_metadata: MetricMetadata object.
          result: iterable of (timestamp, value, count, stage) to modify.
          points: iterable of (timestamp, value).
        """
        aggregator = metric_metadata.aggregator
        for timestamp, value in points:
            step = stage.step(timestamp)
            current_point = result[-1]
            current_step = stage.step(current_point[0])
            if current_step == step:
                # TODO: use a namedtuple.
                # Point is in current step => aggregate:
                #   1. Get current value and and current count.
                #   2. Compute aggregated value with new value.
                #   3. Update stage information.
                # The last point in result now contains up-to-date information.
                current_value = current_point[1]
                current_count = current_point[2]
                aggregated_value = aggregator.merge(current_value, current_count, value)
                result[-1] = (step * stage.precision, aggregated_value, current_count + 1, stage)
            elif current_step < step:
                # Point is in new step => add new step.
                result.append((step * stage.precision, value, 1, stage))

    def _update_stage(self, stage_index, metric_metadata, expired_points, non_expired_points):
        """Compute aggregated values for a stage and store them.

        Only the expired points will be taken into account in the updated
        The points have to be sorted by increasing timestamps.

        Args:
          stage_index: index of stage to update with raw points.
          metric_metadata: MetricMetadata object.
          expired_points: expired raw points to be added to the current stage aggregate.
          non_expired_points: non-expired raw points to be added to the current stage aggregate.

        Returns:
          Iterable of (timestamp, value, count, stage).
        """
        stages = metric_metadata.retention.stages
        stage = stages[stage_index]
        precision = stage.precision

        current_timestamp = self._get_stage_timestamp(stage_index)
        current_value = self._get_stage_value(stage_index)
        current_count = self._get_stage_count(stage_index)
        if current_timestamp == -1:
            if expired_points:
                first_point = expired_points[0]
            elif non_expired_points:
                first_point = non_expired_points[0]
            else:
                return []

            # Raw buffer is empty  => take first point timestamp.
            first_step = stage.step(first_point[0])
            first_timestamp = first_step * precision
            current_timestamp = first_timestamp

        result = [(current_timestamp, current_value, current_count, stage)]

        self._coalesce(stage, metric_metadata, result, expired_points)
        last_point = result[- 1]
        self._set_stage_timestamp(stage_index, last_point[0])
        self._set_stage_value(stage_index, last_point[1])
        self._set_stage_count(stage_index, last_point[2])

        self._coalesce(stage, metric_metadata, result, non_expired_points)

        return result

    def update(self, metric_metadata, points):
        """"Compute aggregated values and store them.

        Only the expired points will be taken into account in the updated
        The points have to be sorted by increasing timestamp.

        Args:
          metric_metadata: MetricMetadata object
          points: iterable of (timestamp, value).

        Returns:
          The list of expired (timestamp, value, count, stage) for all stages.
        """
        retention = metric_metadata.retention
        stages = retention.stages
        (expired_raw, non_expired_raw) = self._update_raw(retention, points)
        result = []
        for stage_index in xrange(len(stages)):
            result_stage = self._update_stage(stage_index,
                                              metric_metadata,
                                              expired_raw,
                                              non_expired_raw)
            result.extend(result_stage)
        return result

    @property
    def _raw_timestamp(self):
        """Get timestamp of raw buffer."""
        return self._timestamps[0]

    @_raw_timestamp.setter
    def _raw_timestamp(self, value):
        """Set timestamp of raw buffer."""
        self._timestamps[0] = value

    def _get_stage_timestamp(self, stage_index):
        """Get timestamp of stage in buffer.

        Raw timestamp is at index 0.
        Stage timestamps start at index 1.

        Args:
          stage_index: stage whose timestamp to get.

        Returns:
          Timestamp of stage, in seconds.
        """
        return self._timestamps[1 + stage_index]

    def _set_stage_timestamp(self, stage_index, timestamp):
        """Set timestamp of stage in buffer.

        Raw timestamp is at index 0.
        Stage timestamps start at index 1.

        Args:
          stage_index: stage whose timestamp to set.
          timestamp: timestamp to set stage to.
        """
        self._timestamps[1 + stage_index] = timestamp

    def _get_stage_value(self, stage_index):
        """Get value of stage in buffer.

        Args:
          stage_index: stage whose value to get.

        Returns:
          Value of stage.
        """
        return self._values[self._raw_capacity + stage_index]

    def _set_stage_value(self, stage_index, value):
        """Set value of stage in buffer.

        Args:
          stage_index: stage whose value to set.
          value: value to set stage to.
        """
        self._values[self._raw_capacity + stage_index] = value

    def _get_stage_count(self, stage_index):
        """Get count of stage in buffer.

        Args:
          stage_index: stage whose count to get.

        Returns:
          Count of stage.
        """
        return self._counts[stage_index]

    def _set_stage_count(self, stage_index, count):
        """Set count of stage in buffer.

        Args:
          stage_index: stage whose count to set.
          count: count to set stage to.
        """
        self._counts[stage_index] = count
