#!/usr/bin/env python
# Copyright 2018 Criteo
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

from __future__ import print_function

import time
import unittest

from biggraphite import metric as bg_metric
from biggraphite.drivers import _delayed_writer as bg_dw
from tests import test_utils

test_utils.setup_logging()


class TestDelayedWriter(test_utils.TestCaseWithFakeAccessor):
    METRIC_NAME = "test.metric.sum"
    PRECISION = 1
    DURATION = 10
    WRITER_PERIOD = 4 * 1000

    def setUp(self):
        """Set up a delayed wrier."""
        super(TestDelayedWriter, self).setUp()
        stages = (self.DURATION, self.PRECISION, self.DURATION, self.PRECISION * 100)
        retention_string = "%d*%ds:%d*%ds" % (stages)
        retention = bg_metric.Retention.from_string(retention_string)
        self.stage_0 = retention.stages[0]
        self.stage_1 = retention.stages[1]

        metadata = bg_metric.MetricMetadata(
            aggregator=bg_metric.Aggregator.average, retention=retention
        )
        self.metric = test_utils.make_metric(self.METRIC_NAME, metadata=metadata)
        self.accessor.create_metric(self.metric)
        self.dw = bg_dw.DelayedWriter(self.accessor, period_ms=self.WRITER_PERIOD)

    def test_feed_simple(self):
        """Test feed with few points."""
        # 1. Put value 1 at timestamp 0.
        # 2. Check that it is used in the aggregates, even though it is not expired.
        points = [(0, 1, 1, self.stage_0), (0, 1, 1, self.stage_1)]
        expected = [(0, 1, 1, self.stage_0)]
        result = self.dw.feed(self.metric, points)
        self.assertEqual(result, expected)

        # 1. Feed no point, and check that nothing is thrown.
        points = []
        result = self.dw.feed(self.metric, points)
        self.assertEqual(result, [])

        # 1. Add point with value 3 that overrides the previous point.
        points = [(1, 3, 1, self.stage_0), (1, 3, 1, self.stage_1)]
        expected = [(1, 3, 1, self.stage_0)]
        result = self.dw.feed(self.metric, points)
        self.assertEqual(result, expected)

        # We should have low res point queued.
        self.assertEqual(self.dw.size(), 1)

        # And none of them written to the accessor.
        points = self.accessor.fetch_points(
            self.metric, 0, 1000, stage=self.metric.retention[1]
        )
        self.assertEqual(len(list(points)), 0)

        # See if we can write the points.
        def _now():
            return time.time() + self.dw.period_ms / 1000

        self.dw.write_some(now=_now)

        # Now we should have points written.
        points = self.accessor.fetch_points(
            self.metric, 0, 1000, stage=self.metric.retention[1]
        )
        self.assertEqual(len(list(points)), 2)

        # And none queued.
        self.assertEqual(self.dw.size(), 0)


if __name__ == "__main__":
    unittest.main()
