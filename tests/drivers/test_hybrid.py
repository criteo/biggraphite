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

import unittest

from mock import Mock

from biggraphite import metric as bg_metric
from biggraphite.drivers import hybrid

DEFAULT_METRIC_NAME = "foo.bar"
DEFAULT_METADATA = bg_metric.MetricMetadata()
DEFAULT_METRIC = bg_metric.Metric(DEFAULT_METRIC_NAME, "id", DEFAULT_METADATA)

DEFAULT_GLOB = "foo.bar.**"


class TestHybridAccessor(unittest.TestCase):
    def setUp(self):
        self._metadata_accessor = Mock()
        self._metadata_accessor.TYPE = 'mock'
        self._data_accessor = Mock()
        self._data_accessor.TYPE = 'mock'
        self._accessor = hybrid.HybridAccessor(
            "test_hybrid", self._metadata_accessor, self._data_accessor
        )

    def test_connect_should_be_called_on_both_accessors(self):
        self._accessor.connect()

        self._metadata_accessor.connect.assert_called_once()
        self._data_accessor.connect.assert_called_once()

    def test_create_metric_should_be_called_only_for_metadata(self):
        self._accessor.connect()
        self._accessor.create_metric(DEFAULT_METRIC)

        self._metadata_accessor.create_metric.assert_called_with(DEFAULT_METRIC)
        self._data_accessor.create_metric.assert_not_called()

    def test_update_metric_should_be_called_only_for_metadata(self):
        self._accessor.connect()
        self._accessor.update_metric(DEFAULT_METRIC, DEFAULT_METADATA)

        self._metadata_accessor.update_metric.assert_called_with(
            DEFAULT_METRIC, DEFAULT_METADATA
        )
        self._data_accessor.update_metric.assert_not_called()

    def test_delete_metric_should_be_called_only_for_metadata(self):
        self._accessor.connect()
        self._accessor.delete_metric(DEFAULT_METRIC)

        self._metadata_accessor.delete_metric.assert_called_with(DEFAULT_METRIC)
        self._data_accessor.delete_metric.assert_not_called()

    def test_delete_directory_should_be_called_only_for_metadata(self):
        self._accessor.connect()
        self._accessor.delete_directory(DEFAULT_METRIC_NAME)

        self._metadata_accessor.delete_directory.assert_called_with(DEFAULT_METRIC_NAME)
        self._data_accessor.delete_directory.assert_not_called()

    def test_drop_all_metrics_should_be_called_on_both_accessors(self):
        self._accessor.connect()
        self._accessor.drop_all_metrics()

        self._metadata_accessor.drop_all_metrics.assert_called_once()
        self._data_accessor.drop_all_metrics.assert_called_once()

    def test_fetch_points_should_be_called_on_both_accessors(self):
        time_start = 0
        time_end = 1
        stage = bg_metric.Stage("0", "1")
        aggregated = False

        self._accessor.connect()
        self._accessor.fetch_points(
            DEFAULT_METRIC, time_start, time_end, stage, aggregated
        )

        self._metadata_accessor.fetch_points.assert_called_with(
            DEFAULT_METRIC, time_start, time_end, stage, aggregated
        )
        self._data_accessor.fetch_points.assert_called_with(
            DEFAULT_METRIC, time_start, time_end, stage, aggregated
        )

    def test_has_metric_should_be_called_only_for_metadata(self):
        self._accessor.connect()
        self._accessor.has_metric(DEFAULT_METRIC_NAME)

        self._metadata_accessor.has_metric.assert_called_with(DEFAULT_METRIC_NAME)
        self._data_accessor.has_metric.assert_not_called()

    def test_get_metric_should_be_called_only_for_metadata(self):
        self._accessor.connect()
        self._accessor.get_metric(DEFAULT_METRIC_NAME)

        self._metadata_accessor.get_metric.assert_called_with(DEFAULT_METRIC_NAME)
        self._data_accessor.get_metric.assert_not_called()

    def test_glob_metrics_should_be_called_only_for_metadata(self):
        self._accessor.connect()
        self._accessor.glob_metrics(DEFAULT_GLOB)

        self._metadata_accessor.glob_metrics.assert_called_with(DEFAULT_GLOB)
        self._data_accessor.glob_metrics.assert_not_called()

    def test_glob_metric_names_should_be_called_only_for_metadata(self):
        self._accessor.connect()
        self._accessor.glob_metric_names(DEFAULT_GLOB)

        self._metadata_accessor.glob_metric_names.assert_called_with(DEFAULT_GLOB)
        self._data_accessor.glob_metric_names.assert_not_called()

    def test_glob_directory_names_should_be_called_only_for_metadata(self):
        self._accessor.connect()
        self._accessor.glob_directory_names(DEFAULT_GLOB)

        self._metadata_accessor.glob_directory_names.assert_called_with(DEFAULT_GLOB)
        self._data_accessor.glob_directory_names.assert_not_called()

    def test_background_should_be_called_on_both_accessors(self):
        self._accessor.connect()
        self._accessor.background()

        self._metadata_accessor.background.assert_called_once()
        self._data_accessor.background.assert_called_once()

    def test_flush_should_be_called_on_both_accessors(self):
        self._accessor.connect()
        self._accessor.flush()

        self._metadata_accessor.flush.assert_called_once()
        self._data_accessor.flush.assert_called_once()

    def test_insert_points_async_should_be_called_only_on_data_accessor(self):
        datapoints = []
        self._accessor.connect()
        self._accessor.insert_points_async(DEFAULT_METRIC, datapoints)

        self._metadata_accessor.insert_points_async.assert_not_called()
        self._data_accessor.insert_points_async.assert_called_with(
            DEFAULT_METRIC, datapoints, None
        )

    def test_insert_downsampled_points_async_should_be_called_only_on_data_accessor(
        self
    ):
        datapoints = []
        self._accessor.connect()
        self._accessor.insert_downsampled_points_async(DEFAULT_METRIC, datapoints)

        self._metadata_accessor.insert_downsampled_points_async.assert_not_called()
        self._data_accessor.insert_downsampled_points_async.assert_called_with(
            DEFAULT_METRIC, datapoints, None
        )

    def test_map_should_be_called_only_for_metadata(self):
        def callback():
            pass

        self._accessor.connect()
        self._accessor.map(callback)

        self._metadata_accessor.map.assert_called_with(callback, None, None, 0, 1, None)
        self._data_accessor.map.assert_not_called()

    def test_repair_should_be_called_only_for_metadata(self):
        self._accessor.connect()
        self._accessor.repair()

        self._metadata_accessor.repair.assert_called_with(None, None, 0, 1, None)
        self._data_accessor.repair.assert_not_called()

    def test_shutdown_should_be_called_on_both_accessors(self):
        self._accessor.connect()
        self._accessor.shutdown()

        self._metadata_accessor.shutdown.assert_called_once()
        self._data_accessor.shutdown.assert_called_once()

    def test_touch_metric_should_be_called_only_for_metadata(self):
        self._accessor.connect()
        self._accessor.touch_metric(DEFAULT_METRIC_NAME)

        self._metadata_accessor.touch_metric.assert_called_with(DEFAULT_METRIC_NAME)
        self._data_accessor.touch_metric.assert_not_called()

    def test_clean_should_be_called_only_for_metadata(self):
        self._accessor.connect()
        self._accessor.clean()

        self._metadata_accessor.clean.assert_called_with(None, None, None, 1, 0, None)
        self._data_accessor.clean.assert_not_called()


if __name__ == "__main__":
    unittest.main()
