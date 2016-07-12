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

"""Metric-level & stage-level aggregate interface."""

from __future__ import print_function


class StageAggregate(object):
    """Perform aggregation on the data of a given retention stage."""

    __slots__ = (
        "precision",
        "_aggregator",
        "_epoch",
        "count",
        "value"
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
        self.count = 0
        self.value = None

    def compute(self, points):
        """Compute aggregated value but do not store it.

        The points have to be sorted by increasing timestamps.

        Args:
          points: new points to be added into the current aggregate.

        Returns:
          Iterable of (timestamp, count, value).
        """
        if self._epoch is None and not points:
            return []

        res = [(self._epoch, self.count, self.value)]
        for (timestamp, value) in points:
            epoch = timestamp // self.precision
            current_point = res[-1]
            current_epoch = current_point[0]
            if current_epoch is None:
                # no prior state => take first point
                res[0] = (epoch, 1, value)
            elif current_epoch == epoch:
                # point is in current epoch => aggregate
                old_value = current_point[2]
                old_count = current_point[1]
                aggregated_value = self._aggregator.merge(old_value, old_count, value)
                res[-1] = (current_point[0], current_point[1] + 1, aggregated_value)
            elif current_epoch < epoch:
                # point is in new epoch => add new epoch
                res.append((epoch, 1, value))
        return [(r[0] * self.precision, r[1], r[2]) for r in res]

    def update(self, points):
        """Compute aggregated value and store it.

        The points have to be sorted by increasing timestamps.

        Args:
          points: new points to be added into the current aggregate.

        Returns:
          Iterable of (timestamp, count, value).
        """
        res = self.compute(points)
        if res:
            # last element of res stores the current state
            current_point = res[-1]
            self._epoch = current_point[0] // self.precision
            self.count = current_point[1]
            self.value = current_point[2]
        return res


class MetricAggregates(object):
    """Perform aggregation on the data of a given metric."""

    __slots__ = (
        "_stage",
    )

    def __init__(self, metric_metadata):
        """Initialize a new MetricAggregator.

        Args:
          metric_metadata: MetricMetadata object.
        """
        self._stage = [StageAggregate(r.precision, metric_metadata.aggregator)
                       for r in metric_metadata.retention]

    def compute(self, points):
        """Compute aggregated values but do not store them.

        The points have to be sorted by increasing timestamps.

        Args:
          points: new points to be added into the current aggregates.

        Returns:
          Iterable of iterables of (timestamp, value).
        """
        # p[0]: timestamp, rounded to a multiple of the stage precision
        # p[2]: aggregated value
        return [[(p[0], p[2]) for p in s.compute(points)]
                for s in self._stage]

    def update(self, points):
        """Compute aggregated values and store them.

        The points have to be sorted by increasing timestamps.

        Args:
          points: new points to be added into the current aggregates.

        Returns:
          Iterable of iterables of (timestamp, value).
        """
        # p[0]: timestamp, rounded to a multiple of the stage precision
        # p[2]: aggregated value
        return [[(p[0], p[2]) for p in s.update(points)]
                for s in self._stage]
