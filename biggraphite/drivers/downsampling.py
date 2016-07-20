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


class MetricDownsampler(object):
    """Metric-bound downsampler using MetricAggregates to produce aggregates."""

    slots = (
        "__metric_buffer",
        "__metric_aggregates"
    )

    def __init__(self, metric_metadata, capacity):
        """Initialize a new MetricDownsampler.

        Args:
          metric_metadata: MetricMetadata object.
          capacity: capacity of the MetricBuffer object.
        """
        stage_0 = metric_metadata.retention[0]
        precision = stage_0.precision
        self.__buffer = MetricBuffer(precision, capacity)
        self.__aggregates = MetricAggregates(metric_metadata)

    def feed(self, datapoints):
        """Feed the downsampler and produce points.

        Points older than the latest ingested point may be ignored.

        Arg:
          metric: Metric
          datapoints: iterable of (timestamp as sec, value as float)
        Returns:
          Iterable of (timestamp, value, count, precision).
        """
        # Sort points by increasing timestamp, because put expects them in order.
        sorted_points = sorted(datapoints)

        # Put points in buffer and pop expired points.
        expired_points = []
        for timestamp, value in sorted_points:
            expired_points.extend(self.__buffer.put(timestamp, value))
        # TODO: remove from __metrics if/when
        # all the points have been removed from the buffer.

        # Update aggregates with expired points.
        aggregates = self.__aggregates.update(expired_points)

        # Compute result with aggregates.
        result = []
        for aggregate in aggregates:
            result.extend(aggregate)
        return result


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

        return self.__metrics[metric.name].feed(datapoints)


class MetricBuffer(object):
    """Perform computation on the data of a given metric."""

    __slots__ = (
        "_precision",
        "_capacity",
        "_buffer",
        "_epoch"
    )

    def __init__(self, precision=60, capacity=10):
        """"Initialize a MetricBuffer object.

        Args:
          precision: precision of the raw buffer in seconds.
          capacity: number of slots in the raw buffer.
        """
        # TODO: use stage in constructor
        self._precision = precision
        self._capacity = capacity
        self._buffer = [None] * capacity
        self._epoch = None

    def current_points(self):
        """Get list of points currently in the buffer.

        Returns:
          The list of (timestamp, value) of points in the buffer.
        """
        if self._epoch is None:
            return []
        res = []
        epoch_start = self._epoch - (self._capacity - 1)
        epoch_end = epoch_start + self._capacity
        for e in xrange(epoch_start, epoch_end):
            index = e % self._capacity
            if self._buffer[index] is not None:
                res.append((e * self._precision, self._buffer[index]))
        return res

    def pop_expired(self, timestamp):
        """"Pop expired elements strictly older than timestamp.

        Args:
          timestamp: time horizon to clear elements, in seconds.

        Returns:
          The list of (timestamp, value) popped from the buffer.
        """
        if self._epoch is None:
            return []
        res = []
        epoch_start = self._epoch - (self._capacity - 1)
        epoch_end = timestamp // self._precision
        for e in xrange(epoch_start, epoch_end):
            index = e % self._capacity
            if self._buffer[index] is not None:
                res.append((e * self._precision, self._buffer[index]))
                self._buffer[index] = None
        return res

    def get(self, timestamp):
        """"Get a data point from the raw buffer.

        Args:
          timestamp: timestamp of the data point.

        Returns:
          The value of the data point at the requested time,
          or None if it's not in the raw buffer.
        """
        if self._epoch is None:
            return None
        epoch = timestamp // self._precision
        epoch_start = self._epoch - (self._capacity - 1)
        epoch_end = self._epoch
        if epoch >= epoch_start and epoch <= epoch_end:
            return self._buffer[epoch % self._capacity]
        return None

    def put(self, timestamp, value):
        """"Insert a data point in the raw buffer and pop expired points.

        Args:
          timestamp: timestamp of the data point, in seconds.
          value: value of the data point.

        Returns:
          The list of expired (timestamp, value) from the raw buffer.
        """
        expiry_timestamp = timestamp - (self._capacity - 1) * self._precision
        expired = self.pop_expired(expiry_timestamp)
        epoch = timestamp // self._precision
        if self._epoch is None:
            self._epoch = epoch
        if epoch > self._epoch - self._capacity:
            self._buffer[epoch % self._capacity] = value
        if epoch > self._epoch:
            self._epoch = epoch
        return expired


class StageAggregate(object):
    """Perform aggregation on the data of a given retention stage."""

    __slots__ = (
        "precision",
        "_aggregator",
        "_epoch",
        "value",
        "count"
    )

    def __init__(self, precision, aggregator):
        """Initialize a new StageAggregator.

        Args:
          precision: precision of the stage, in seconds.
          aggregator: aggregator object to compute aggregates.
        """
        self.precision = precision
        self._aggregator = aggregator
        self._epoch = None
        self.value = None
        self.count = 0

    def compute(self, points):
        """Compute aggregated value but do not store it.

        The points have to be sorted by increasing timestamps.

        Args:
          points: new points to be added into the current aggregate.

        Returns:
          Iterable of (timestamp, value, count, precision).
        """
        # No prior state and no update => return empty list.
        if self._epoch is None and not points:
            return []

        res = [(self._epoch, self.value, self.count, self.precision)]
        for (timestamp, value) in points:
            epoch = timestamp // self.precision
            current_point = res[-1]
            current_epoch = current_point[0]
            if current_epoch is None:
                # no prior state => take first point
                res[0] = (epoch, value, 1, self.precision)
            elif current_epoch == epoch:
                # point is in current epoch => aggregate:
                #   1. get current value from last point in res
                #   2. get current count from last point in res
                #   3. compute aggregated value with new value
                #   4. replace last point in res
                # the last point in res now contains up-to-date information
                current_value = current_point[1]
                current_count = current_point[2]
                aggregated_value = self._aggregator.merge(current_value, current_count, value)
                res[-1] = (current_epoch, aggregated_value, current_count + 1, self.precision)
            elif current_epoch < epoch:
                # point is in new epoch => add new epoch
                res.append((epoch, value, 1, self.precision))
        return [(r[0] * self.precision, r[1], r[2], r[3]) for r in res]

    def update(self, points):
        """Compute aggregated value and store it.

        The points have to be sorted by increasing timestamps.

        Args:
          points: new points to be added into the current aggregate.

        Returns:
          Iterable of (timestamp, value, count, precision).
        """
        res = self.compute(points)
        if res:
            # update current state with last element of res
            current_point = res[-1]
            self._epoch = current_point[0] // self.precision
            self.value = current_point[1]
            self.count = current_point[2]
        return res


class MetricAggregates(object):
    """Perform aggregation on the data of a given metric."""

    __slots__ = (
        "_stages",
    )

    def __init__(self, metric_metadata):
        """Initialize a new MetricAggregator.

        Args:
          metric_metadata: MetricMetadata object.
        """
        self._stages = [StageAggregate(r.precision, metric_metadata.aggregator)
                        for r in metric_metadata.retention]

    def compute(self, points):
        """Compute aggregated values but do not store them.

        The points have to be sorted by increasing timestamps.

        Args:
          points: new points to be added into the current aggregates.

        Returns:
          Iterable of iterables of (timestamp, value, count, precision).
        """
        return [[p for p in s.compute(points)] for s in self._stages]

    def update(self, points):
        """Compute aggregated values and store them.

        The points have to be sorted by increasing timestamps.

        Args:
          points: new points to be added into the current aggregates.

        Returns:
          Iterable of iterables of (timestamp, value, count, precision).
        """
        return [[p for p in s.update(points)] for s in self._stages]
