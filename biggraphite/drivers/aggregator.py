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

"""Downsampling interface."""

from __future__ import print_function


class StageAggregator(object):
    """Perform aggregation on the data of a given retention stage."""

    __slots__ = (
        "precision",
        "_aggregator",
        "_epoch",
        "_count",
        "_value"
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
        self._count = 0
        self._value = None

    def compute_aggregate(self, points):
        """Compute aggregated value but do not store it.
        The points have to be sorted by increasing timestamps.

        Args:
          points: new points to be added into the current aggregate.

        Returns:
          Array of (timestamp, count, value).
        """
        if self._epoch is None and not points:
            return []

        res = [(self._epoch, self._count, self._value)]
        for (timestamp, value) in points:
            epoch = timestamp // self.precision
            res_epoch = res[-1][0]
            if res_epoch is None:
                res[0] = (epoch, 1, value)
            elif res_epoch < epoch:
                res.append((epoch, 1, value))
            else:
                values = [res[-1][2], value]
                counts = [res[-1][1], 1]
                aggregated_value = self._aggregator.downsample(values, counts)
                res[-1] = (res[-1][0], res[-1][1] + 1, aggregated_value)
        return [(r[0] * self.precision, r[1], r[2]) for r in res]

    def update_aggregate(self, points):
        """Compute aggregated value and store it.
        The points have to be sorted by increasing timestamps.

        Args:
          points: new points to be added into the current aggregate.

        Returns:
          Array of (timestamp, count, value).
        """
        res = self.compute_aggregate(points)
        if res:
            self._epoch = res[-1][0] // self.precision
            self._count = res[-1][1]
            self._value = res[-1][2]
        return res


class MetricAggregator(object):
    """Perform aggregation on the data of a given metric."""

    __slots__ = (
        "_stage_aggregators",
    )

    def __init__(self, metric_metadata):
        """Initialize a new MetricAggregator.

        Args:
          metric_metadata: MetricMetadata object.
        """
        self._stage_aggregators = [StageAggregator(r.precision, metric_metadata.aggregator)
                                   for r in metric_metadata.retention]

    def compute_aggregates(self, points):
        """Compute aggregated values but do not store them.
        The points have to be sorted by increasing timestamps.

        Args:
          points: new points to be added into the current aggregates.

        Returns:
          Array of arrays of (timestamp, value).
        """
        return [[(p[0], p[2]) for p in s.compute_aggregate(points)]
                for s in self._stage_aggregators]

    def update_aggregates(self, points):
        """Compute aggregated values and store them.
        The points have to be sorted by increasing timestamps.

        Args:
          points: new points to be added into the current aggregates.

        Returns:
          Array of arrays of (timestamp, value).
        """
        return [[(p[0], p[2]) for p in s.update_aggregate(points)]
                for s in self._stage_aggregators]
