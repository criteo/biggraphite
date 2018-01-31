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

import collections
import logging
import time


log = logging.getLogger(__name__)


class DelayedWriter(object):
    """Delay writes."""

    DEFAULT_PERIOD_MS = 600000

    def __init__(self, accessor, period_ms=DEFAULT_PERIOD_MS):
        """Create a DelayedWriter.

        The delayed writer will separate high resolution points and low
        resolution points and will write the low resolution ones every
        `period_ms` milliseconds.

        For these points the value for a given timestamp is frequently
        updated and we can safely delay the writes. In case of an unclean
        shutdown we might loose up to `period_ms` points of data.

        Args:
          accessor: a connected accessor.
          period_ms: delay before writing low resolution points.
        """
        self.accessor = accessor
        self.period_ms = period_ms
        self._queue = []
        self._metrics_per_ms = 0
        self._last_write_ms = 0
        self._points = collections.defaultdict(dict)

    def clear(self):
        """Reset internal structures."""
        self._queue = []
        self._points.clear()

    def feed(self, metric, datapoints):
        """Feed the delayed writer.

        This function will seperate datapoints based on their
        resolutions and keep the low resolution points for later.

        Args:
          metric: the metric associated with these points.
          datapoints: downsampled datapoints.

        Returns:
          list(datapoints) list of high resolution points that
            should get written now.
        """
        high_res, low_res = [], []
        for datapoint in datapoints:
            _, _, _, stage = datapoint
            # In case of unclean shutdown we could loose up to
            # 25% of the data. We also allow a lag of up to 1/4th of
            # a period. stage0 are never delayed.
            if stage.stage0 or stage.precision_ms < (self.period_ms * 4):
                high_res.append(datapoint)
            else:
                low_res.append(datapoint)
        self.write_later(metric, low_res)
        # We piggy back on feed() to write delayed points, this works
        # as long as we receive points regularly. We might want to add
        # a timer at some point.
        self.write_some()
        return high_res

    def flush(self):
        """Flush all buffered points."""
        self._build_queue()
        while self._queue:
            self.write_some(flush=True)

    def size(self):
        """Number of queued metrics."""
        return len(self._points)

    def write_later(self, metric, datapoints):
        """Queue points for later."""
        for datapoint in datapoints:
            timestamp, value, count, stage = datapoint
            self._points[metric][(stage, timestamp)] = (value, count)
        self._build_queue()

    def _build_queue(self):
        """Build the queue of metrics to write."""
        if len(self._queue) > 0:
            return
        # Order by number of points.
        self._queue = sorted(
            self._points.keys(),
            key=lambda k: len(self._points[k])
        )
        # We know that we have up to `period_ms` to write everything
        # so let's write only a few metrics per iteration.
        self._metrics_per_ms = float(len(self._queue)) / self.period_ms
        log.debug("rebuilt the queues: %d metrics, %d per second",
                  len(self._queue), self._metrics_per_ms)

    def write_some(self, flush=False, now=time.time):
        """Write some points from the queue."""
        now = now() * 1000  # convert to ms.
        if self._last_write_ms == 0:
            self._last_write_ms = now
        delta_ms = (now - self._last_write_ms) + 1
        if flush:
            metrics_to_write = len(self._queue)
        else:
            metrics_to_write = round(delta_ms * self._metrics_per_ms)
        if metrics_to_write == 0:
            return
        i = 0
        log.debug("writing low res points for %d metrics" % metrics_to_write)
        while self._queue and i < metrics_to_write:
            metric = self._queue.pop()
            datapoints = []
            # collect the points to write them.
            for k, v in self._points[metric].items():
                stage, timestamp = k
                value, count = v
                i += 1
                datapoints.append((timestamp, value, count, stage))
            self.accessor.insert_downsampled_points_async(metric, datapoints)
            # remove the points that have been written
            del self._points[metric]
        self._last_write_ms = now
