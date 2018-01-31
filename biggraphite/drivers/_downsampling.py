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
import logging
import math
import time

log = logging.getLogger(__name__)
_NaN = float("NaN")


class Downsampler(object):
    """Downsampler using MetricAggregates to produce aggregates."""

    CAPACITY = 20
    PURGE_EVERY_S = 3600

    slots = (
        "_capacity",
        "_names_to_aggregates"
    )

    def __init__(self, capacity=CAPACITY):
        """Default constructor."""
        self._capacity = capacity
        self._names_to_aggregates = {}
        self._last_purge = 0

    def feed(self, metric, points):
        """Feed the downsampler and produce points.

        All points returned by this method must be persisted or they will
        be lost. Some of them are likely to be re-emitted for stages with
        long resolution.

        Arg:
          metric: Metric
          points: iterable of (timestamp, value)

        Returns:
          Iterable of (timestamp, value, count, precision).
        """
        # Ensure we have the required aggregation mechanism.
        if metric.name not in self._names_to_aggregates:
            metric_aggregates = MetricAggregates(
                metric.metadata, stage0_capacity=self._capacity)
            self._names_to_aggregates[metric.name] = metric_aggregates

        # Sort points by increasing timestamp, because put expects them in order.
        return self._names_to_aggregates[metric.name].update(sorted(points))

    def clear(self):
        """Clear internal structures."""
        self._names_to_aggregates.clear()
        self._last_purge = 0

    def purge(self, now=time.time()):
        """Purge unused data."""
        if now - self._last_purge <= self.PURGE_EVERY_S:
            return

        self._last_purge = now
        for metric_name in list(self._names_to_aggregates):
            aggregate = self._names_to_aggregates[metric_name]
            if aggregate.obsolete(now):
                del self._names_to_aggregates[metric_name]
                log.info('removing obsolete aggregates for %s' % metric_name)


class MetricAggregates(object):
    """Perform aggregation on metric points."""

    DEFAULT_STAGE0_CAPACITY = 10

    __slots__ = (
        "_metric_metadata",
        "_stage0_capacity",
        "_timestamps",
        "_values",
        "_counts",
    )

    def __init__(self, metric_metadata, stage0_capacity=DEFAULT_STAGE0_CAPACITY):
        """Initialize a MetricAggregates object.

        Args:
          metric_metadata: MetricMetadata object.
          precision: precision of the stage0 buffer in seconds.
          capacity: number of slots in the stage0 buffer.
        """
        self._metric_metadata = metric_metadata

        stages = len(metric_metadata.retention.stages)

        # _stage0_capacity: length of the stage0 buffer
        self._stage0_capacity = stage0_capacity

        # _timestamps: array of integers:
        #   - stage0 timestamp at index 0
        #   - stage timestamp from index 1
        # TODO: make sure that each stage timestamp is equal to:
        # (stage0_timestamp - (stage0_capacity - 1) * stage0_precision) % stage_precision
        self._timestamps = array.array("i", [-1] * (1 + stages))

        # _values: array of doubles:
        #   - stage0 buffer values from index 0 with length stage0_capacity
        #   - stage values from index stage0_capacity
        self._values = array.array("d", [_NaN] * (stage0_capacity + stages))

        # _counts: array of integer: stage counts from index 0
        self._counts = array.array("i", [0] * stages)

    def _get_expired_stage0_points(self, points):
        """Put stage0 data points in stage0 buffer and return expired stage0 points.

        Args:
          points: iterable of (timestamp, value).

        Returns:
          (updated, expired)
          - updated: The list of (timestamp, value, count, stage) of updated stage0 points.
          - expired: The list of (timestamp, value) of expired stage0 points.
        """
        if self._stage0_timestamp == -1:
            # Stage0 buffer is empty.
            if not points:
                # No update => nothing to expire.
                return ([], [])
            # Otherwise, update stage0 timestamp to first point.
            first_datapoint = points[0]
            first_timestamp = first_datapoint[0]
            self._stage0_timestamp = first_timestamp

        stage_0 = self._stage0
        expired = []
        updated = {}
        for timestamp, value in points:
            last_update_step = stage_0.step(self._stage0_timestamp)
            point_step = stage_0.step(timestamp)
            point_timestamp = stage_0.round_down(timestamp)
            point_index = point_step % self._stage0_capacity
            downsampled_point = (point_timestamp, value, 1, self._stage0)
            if point_step > last_update_step:
                # Point is more recent than most recent stage0 point => expire.
                # If we add N points, we have to expire N points.
                # Here, N = point_step - last_update_step.
                expired_count = point_step - last_update_step

                # However, N can be larger than the stage0 buffer capacity.
                # But we only need to expire as many points as the stage0 buffer capacity.
                expired_count = min(expired_count, self._stage0_capacity)

                # The first point to expire is the oldest.
                start_step = last_update_step - (self._stage0_capacity - 1)
                end_step = start_step + expired_count
                for step in range(start_step, end_step):
                    index = step % self._stage0_capacity
                    if not math.isnan(self._values[index]):
                        expired.append(
                            (step * stage_0.precision, self._values[index]))
                    self._values[index] = _NaN
                self._stage0_timestamp = point_timestamp

                self._values[point_index] = value
                updated[point_step] = downsampled_point
            elif point_step > last_update_step - self._stage0_capacity:
                # Point fits in the buffer => replace value in the stage0 buffer.

                if (self._values[point_index] != value):
                    updated[point_step] = downsampled_point
                self._values[point_index] = value

        # Sort based on timestamp.
        updated = sorted(updated.values(), key=lambda v: v[0])
        return updated, expired

    def _get_non_expired_stage0_points(self):
        """Get current (non expired) stage0 points.

        Returns:
          The list of (timestamp, value) of non-expired stage0 points.
        """
        if self._stage0_timestamp == -1:
            # Stage0 buffer is empty.
            return []

        result = []
        stage_0 = self._stage0
        # Most ancient step is at the beginning of the stage0 buffer,
        # which is current step - (length - 1)
        start_step = stage_0.step(
            self._stage0_timestamp) - (self._stage0_capacity - 1)
        end_step = start_step + self._stage0_capacity
        for step in range(start_step, end_step):
            index = step % self._stage0_capacity
            if not math.isnan(self._values[index]):
                # There is a value for this step => append to result buffer.
                result.append((step * stage_0.precision, self._values[index]))

        return result

    def _update_stage0(self, points):
        """Put stage0 points in stage0 buffer and return all stage0 points.

        Args:
          points: iterable of (timestamp, value).

        Returns:
          A tuple of (updated, expired, non expired) stage0 points.
          Elements of updated are (timestamp, value, count, stage) which
          represent 'downsampled' stage0 points.
          The others are list of (timestamp, value) of stage0 points.
        """
        updated, expired = self._get_expired_stage0_points(points)
        non_expired = self._get_non_expired_stage0_points()

        return (updated, expired, non_expired)

    def _merge(self, stage, result, points):
        """Merge stage0 points into result.

        Args:
          stage: Stage object.
          result: iterable of (timestamp, value, count, stage) to modify.
          points: iterable of (timestamp, value).
        """
        aggregator = self._metric_metadata.aggregator
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
                _, current_value, current_count, _ = current_point
                values = [current_value, value]
                counts = [current_count, 1]
                value, count = aggregator.merge(values, counts)
                result[-1] = (step * stage.precision, value, count, stage)
            elif current_step < step:
                # Point is in new step => add new step.
                result.append((step * stage.precision, value, 1, stage))

    def _update_stage(self, stage_index, expired_points, non_expired_points):
        """Compute aggregated values for a stage and store them.

        Only the expired points will be taken into account in the updated
        The points have to be sorted by increasing timestamps.

        Args:
          stage_index: index of stage to update with stage0 points.
          expired_points: expired stage0 points to be added to the current stage aggregate.
          non_expired_points: non-expired stage0 points to be added to the current stage
            aggregate.

        Returns:
          Iterable of (timestamp, value, count, stage).
        """
        stages = self._metric_metadata.retention.stages
        stage = stages[stage_index]
        precision = stage.precision

        # Get current information of the stage to update.
        current_timestamp = self._get_stage_timestamp(stage_index)
        current_value = self._get_stage_value(stage_index)
        current_count = self._get_stage_count(stage_index)

        if current_timestamp == -1:
            # There is no prior information => determine first point to merge into buffer.
            if expired_points:
                first_point = expired_points[0]
            elif non_expired_points:
                first_point = non_expired_points[0]
            else:
                # No prior stage and no new points => nothing to return.
                return []

            # Stage0 buffer is empty  => take first point timestamp.
            first_step = stage.step(first_point[0])
            first_timestamp = first_step * precision
            current_timestamp = first_timestamp

        result = [(current_timestamp, current_value, current_count, stage)]

        # Update result with expired points.
        self._merge(stage, result, expired_points)
        # At this point, result contains only expired points => update stage information.
        # The last point in result contains the information taking into account
        # the expired points, so we use it to update the stage information.
        last_point = result[- 1]
        self._set_stage_timestamp(stage_index, last_point[0])
        self._set_stage_value(stage_index, last_point[1])
        self._set_stage_count(stage_index, last_point[2])

        # Update result with non-expired points.
        # Don't update the stage information here.
        self._merge(stage, result, non_expired_points)

        return result

    def update(self, points):
        """Compute aggregated values and store them.

        Only the expired points will be taken into account in the updated
        The points have to be sorted by increasing timestamp.

        Args:
          points: iterable of (timestamp, value).

        Returns:
          The list of expired (timestamp, value, count, stage) for all stages.
        """
        # TODO(c.chary): Check if metadata has changed and update internal
        #   structures. This will allow us to change retention policies without
        #   restarting everything.
        retention = self._metric_metadata.retention
        stages = retention.stages

        (result, expired_stage0, non_expired_stage0) = (
            self._update_stage0(points))

        # Early return if we haven't changed anything.
        if not result:
            return result

        # Update all the other stages.
        for stage_index in range(1, len(stages)):
            result_stage = self._update_stage(
                stage_index, expired_stage0, non_expired_stage0)
            result.extend(result_stage)

        return result

    @property
    def _stage0(self):
        """Get stage0."""
        return self._retention[0]

    @property
    def _retention(self):
        """Get retentions."""
        return self._metric_metadata.retention

    def obsolete(self, now):
        """Check if this aggregate is obsolete."""
        if not len(self._retention.downsampled_stages):
            # If there is only one stage, keep it for stage0_capacity periods.
            keepalive = (self._stage0.precision * self._stage0_capacity)
        else:
            # Else, keep it for at least twice the precision of the upper stage.
            keepalive = (self._retention[1].precision * 2)
        return self._stage0_timestamp < now - keepalive

    @property
    def _stage0_timestamp(self):
        """Get timestamp of stage0 buffer."""
        return self._timestamps[0]

    @_stage0_timestamp.setter
    def _stage0_timestamp(self, value):
        """Set timestamp of stage0 buffer."""
        self._timestamps[0] = value

    def _get_stage_timestamp(self, stage_index):
        """Get timestamp of stage in buffer.

        Stage0 timestamp is at index 0.
        Stage timestamps start at index 1.

        Args:
          stage_index: stage whose timestamp to get.

        Returns:
          Timestamp of stage, in seconds.
        """
        return self._timestamps[1 + stage_index]

    def _set_stage_timestamp(self, stage_index, timestamp):
        """Set timestamp of stage in buffer.

        Stage0 timestamp is at index 0.
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
        return self._values[self._stage0_capacity + stage_index]

    def _set_stage_value(self, stage_index, value):
        """Set value of stage in buffer.

        Args:
          stage_index: stage whose value to set.
          value: value to set stage to.
        """
        self._values[self._stage0_capacity + stage_index] = value

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
