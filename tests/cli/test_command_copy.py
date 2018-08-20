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

import unittest

from biggraphite import metric as bg_metric
from biggraphite.cli import command_copy
from biggraphite.cli.command_list import list_metrics
from tests import test_utils as bg_test_utils


class TestCommandCopy(bg_test_utils.TestCaseWithFakeAccessor):
    _POINTS_START = 3600 * 24 * 10
    _POINTS_END = _POINTS_START + 3 * 3600
    _RETENTION = bg_metric.Retention.from_string("20*15s:1440*60s:48*3600s")
    _RETENTION_BIS = bg_metric.Retention.from_string("20*10s:14400*60s:500*3600s")
    _POINTS = bg_test_utils._make_easily_queryable_points(
        start=_POINTS_START, end=_POINTS_END, period=_RETENTION[1].precision
    )
    _METRIC_1_NAME = "test.origin.metric_1.toto"
    _METRIC_1 = bg_test_utils.make_metric(_METRIC_1_NAME, retention=_RETENTION)
    _METRIC_2_NAME = "test.origin.metric_2.tata"
    _METRIC_2 = bg_test_utils.make_metric(_METRIC_2_NAME, retention=_RETENTION)
    _METRIC_3_NAME = "test.origin.metric_3.tata"
    _METRIC_3 = bg_test_utils.make_metric(_METRIC_3_NAME, retention=_RETENTION_BIS)

    def setUp(self):
        """Set up a subdirectory of metrics to copy."""
        super(TestCommandCopy, self).setUp()
        self.accessor.connect()
        self.accessor.create_metric(self._METRIC_1)
        self.accessor.create_metric(self._METRIC_2)
        self.accessor.insert_points(self._METRIC_1, self._POINTS)
        self.accessor.flush()

    def test_copy_metric(self):
        """Test copy of a single metric with aggregated points."""
        cmd_copy = command_copy.CommandCopy()

        # Chack that _METRIC_2 is empty
        for i in range(3):
            pts = self.accessor.fetch_points(
                self._METRIC_2,
                self._POINTS_START,
                self._POINTS_END,
                stage=self._METRIC_2.retention[i],
            )
            self.assertEqual(list(pts), [])

        # Copy points from _METRIC_1 to _METRIC_2
        cmd_copy._copy_metric(
            self.accessor,
            self._METRIC_1,
            self._METRIC_2,
            self._POINTS_START,
            self._POINTS_END,
        )
        self.accessor.flush()

        # Check that both metrics have same points
        for i in range(3):
            pts = self.accessor.fetch_points(
                self._METRIC_1,
                self._POINTS_START,
                self._POINTS_END,
                stage=self._METRIC_1.retention[i],
                aggregated=False,
            )
            pts_copy = self.accessor.fetch_points(
                self._METRIC_2,
                self._POINTS_START,
                self._POINTS_END,
                stage=self._METRIC_2.retention[i],
                aggregated=False,
            )
            self.assertEqual(list(pts), list(pts_copy))

    def test_copy_metric_with_retention(self):
        """Test copy of a metric with aggregated points and retention override.

        A given dst_stage should have the same points of the src_stage
        that have the same precision, or no point at all.
        """
        cmd_copy = command_copy.CommandCopy()
        cmd_copy._copy_metric(
            self.accessor,
            self._METRIC_1,
            self._METRIC_3,
            self._POINTS_START,
            self._POINTS_END,
        )
        self.accessor.flush()
        for i in range(3):
            pts = self.accessor.fetch_points(
                self._METRIC_1,
                self._POINTS_START,
                self._POINTS_END,
                stage=self._METRIC_1.retention[i],
                aggregated=False,
            )
            pts_copy = self.accessor.fetch_points(
                self._METRIC_3,
                self._POINTS_START,
                self._POINTS_END,
                stage=self._METRIC_3.retention[i],
                aggregated=False,
            )
            if i == 0:
                self.assertNotEqual(list(pts), list(pts_copy))
            else:
                self.assertEqual(list(pts), list(pts_copy))

    def test_get_metric_tuples_with_metric(self):
        """Test retrieve of a single couple of metrics."""
        cmd_copy = command_copy.CommandCopy()

        # Test with metric names arguments
        expected_metric_tuples = [(self._METRIC_1, self._METRIC_2)]
        metric_tuples = cmd_copy._get_metric_tuples(
            accessor=self.accessor,
            src=self._METRIC_1_NAME,
            dst=self._METRIC_2_NAME,
            src_retention="",
            dst_retention="",
            recursive=False,
            dry_run=False,
        )
        self.assertEqual(list(metric_tuples), expected_metric_tuples)

    def test_get_metric_tuples_with_directory(self):
        """Test retrieve of a single couple of metrics."""
        cmd_copy = command_copy.CommandCopy()
        # Test with subdirectory names arguments
        self.assertEqual(len(list(list_metrics(self.accessor, "*.**"))), 2)
        metric_tuples = cmd_copy._get_metric_tuples(
            accessor=self.accessor,
            src="test",
            dst="copy",
            src_retention="",
            dst_retention="",
            recursive=True,
            dry_run=False,
        )
        self.assertEqual(len(list(metric_tuples)), 2)
        self.assertEqual(len(list(list_metrics(self.accessor, "*.**"))), 4)

    def test_get_metric_tuples_with_retention(self):
        """Test retrieve of a single couples of metrics overrinding retentions."""
        cmd_copy = command_copy.CommandCopy()
        metric_tuples = cmd_copy._get_metric_tuples(
            accessor=self.accessor,
            src=self._METRIC_1_NAME,
            dst=self._METRIC_2_NAME,
            src_retention="18*42s",
            dst_retention="50*300s",
            recursive=False,
            dry_run=False,
        )
        retention_str = [m.metadata.retention.as_string for m in list(metric_tuples)[0]]
        self.assertEqual(len(retention_str), 2)
        self.assertIn("18*42s", retention_str)
        self.assertIn("50*300s", retention_str)


if __name__ == "__main__":
    unittest.main()
