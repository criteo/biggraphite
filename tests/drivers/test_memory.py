#!/usr/bin/env python
# coding=utf-8
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

from biggraphite.metric import Aggregator, Retention, MetricMetadata
from tests import test_utils as bg_test_utils


class TestInMemoryAccessor(bg_test_utils.TestCaseWithAccessor):
    ACCESSOR_SETTINGS = {"driver": "memory"}

    def test_glob_metric_names_should_return_names_matching_glob(self):
        metric_name = "test_glob_metric_names_should_return_names_matching_glob.a.b.c"
        metric = bg_test_utils.make_metric(metric_name, _create_default_metadata())
        self.accessor.create_metric(metric)
        self.accessor.flush()

        metric_names = self.accessor.glob_metric_names("*.a.b.c")

        self.assertGreaterEqual(len(metric_names), 1, "Expected metric not found")
        found_metric_name = metric_names[0]
        self.assertEqual(found_metric_name, metric_name)

    def test_glob_metric_names_should_not_return_names_not_matching_glob(self):
        metric_name = "test_glob_metric_names_should_not_return_names_not_matching_glob.a.b.c"
        metric = bg_test_utils.make_metric(metric_name, _create_default_metadata())
        self.accessor.create_metric(metric)
        self.accessor.flush()

        metric_names = self.accessor.glob_metric_names("*.foo.bar")

        self.assertEqual(len(metric_names), 0, "No metric should have been found")

    def test_glob_metrics_should_return_metrics_matching_glob(self):
        metric_name = "test_glob_metrics_should_return_metrics_matching_glob.a.b.c"
        metric = bg_test_utils.make_metric(metric_name, _create_default_metadata())
        self.accessor.create_metric(metric)
        self.accessor.flush()

        found_metrics = self.accessor.glob_metrics("*.a.b.c")

        self.assertGreaterEqual(len(found_metrics), 1, "Expected metric not found")
        found_metric = found_metrics[0]
        self.assertEqual(found_metric.name, metric_name)

    def test_glob_metrics_should_not_return_metrics_not_matching_glob(self):
        metric_name = "test_glob_metrics_should_not_return_metrics_not_matching_glob.a.b.c"
        metric = bg_test_utils.make_metric(metric_name, _create_default_metadata())
        self.accessor.create_metric(metric)
        self.accessor.flush()

        metric_names = self.accessor.glob_metrics("*.foo.bar")

        self.assertEqual(len(metric_names), 0, "No metric should have been found")


def _create_default_metadata():
    aggregator = Aggregator.maximum
    retention_str = "42*1s:43*60s"
    retention = Retention.from_string(retention_str)
    carbon_xfilesfactor = 0.5
    metadata = MetricMetadata(aggregator, retention, carbon_xfilesfactor)
    return metadata
