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

from tests import test_utils as bg_test_utils  # noqa

bg_test_utils.prepare_graphite_imports()  # noqa

import unittest

from carbon import database
from carbon import conf as carbon_conf

from biggraphite import metric as bg_metric
from biggraphite.plugins import carbon as bg_carbon


_TEST_METRIC = "mytestmetric"


class TestCarbonDatabase(bg_test_utils.TestCaseWithFakeAccessor):
    def setUp(self):
        super(TestCarbonDatabase, self).setUp()
        self.fake_drivers()
        settings = carbon_conf.Settings()
        settings["BG_CASSANDRA_CONTACT_POINTS"] = "host1,host2"
        settings["BG_CASSANDRA_KEYSPACE"] = self.KEYSPACE
        settings["STORAGE_DIR"] = self.tempdir
        self._plugin = bg_carbon.BigGraphiteDatabase(settings)

        def _create(metric, metric_name):
            self._plugin.cache.create_metric(metric)

        # Make sure we don't create metrics asynchronously
        self._plugin._createAsyncOrig = self._plugin._createAsync
        self._plugin._createAsync = _create

        self._plugin.create(
            _TEST_METRIC,
            retentions=[(1, 60)],
            xfilesfactor=0.5,
            aggregation_method="sum",
        )

    def test_empty_settings(self):
        bg_carbon.BigGraphiteDatabase(carbon_conf.Settings())

    def test_get_fs_path(self):
        path = self._plugin.getFilesystemPath(_TEST_METRIC)
        self.assertTrue(path.startswith("//biggraphite/"))
        self.assertIn(_TEST_METRIC, path)

    def test_create_get(self):
        other_metric = _TEST_METRIC + "-other"
        self._plugin.create(
            other_metric,
            retentions=[(1, 60)],
            xfilesfactor=0.5,
            aggregation_method="average",
        )
        self.assertTrue(self._plugin.exists(other_metric))

        aggr = self._plugin.getMetadata(other_metric, "aggregationMethod")
        self.assertEqual("average", aggr)

    def test_update_metric(self):
        other_metric = _TEST_METRIC + "-other"
        self._plugin.create(
            other_metric,
            retentions=[(1, 60)],
            xfilesfactor=0.5,
            aggregation_method="average",
        )

        self.assertTrue(self._plugin.exists(other_metric))
        aggr = self._plugin.getMetadata(other_metric, "aggregationMethod")
        self.assertEqual("average", aggr)

        self._plugin.create(
            other_metric,
            retentions=[(1, 60)],
            xfilesfactor=0.5,
            aggregation_method="sum",
        )

        self.assertTrue(self._plugin.exists(other_metric))
        aggr = self._plugin.getMetadata(other_metric, "aggregationMethod")
        self.assertEqual("sum", aggr)

    def test_create_async(self):
        metric_name = "a.b.c"
        metric = bg_test_utils.make_metric(metric_name)

        self._plugin._createAsyncOrig(metric, metric_name)
        self.assertFalse(self._plugin.exists(metric_name))
        self._plugin._createOneMetric()
        self.assertTrue(self._plugin.exists(metric_name))

        # See if we can update.
        metric = bg_test_utils.make_metric(metric_name)
        metric.metadata.retention = bg_metric.Retention([bg_metric.Stage(1, 1)])
        self._plugin._createAsyncOrig(metric, metric_name)
        self._plugin._createOneMetric()
        retention = self._plugin.getMetadata(metric_name, "retention")
        self.assertEqual(retention, metric.metadata.retention)

    def test_nosuchmetric(self):
        other_metric = _TEST_METRIC + "-nosuchmetric"
        self.assertRaises(
            ValueError,
            self._plugin.setMetadata,
            other_metric,
            "aggregationMethod",
            "avg",
        )
        self.assertRaises(
            ValueError, self._plugin.getMetadata, other_metric, "aggregationMethod"
        )

    def test_getMetadata(self):
        self.assertEqual(
            self._plugin.getMetadata(_TEST_METRIC, "carbon_xfilesfactor"), 0.5
        )
        self.assertRaises(
            ValueError, self._plugin.getMetadata, _TEST_METRIC, "unsupportedMetadata"
        )
        # Specific behavior for aggregationMethod metadata
        self.assertEqual(
            self._plugin.getMetadata(_TEST_METRIC, "aggregationMethod"), "sum"
        )

    def test_setMetadata(self):
        # Setting the same value should work
        self._plugin.setMetadata(_TEST_METRIC, "aggregationMethod", "sum")
        self.assertEqual(
            self._plugin.getMetadata(_TEST_METRIC, "aggregationMethod"), "sum"
        )
        self.assertEqual(
            self._plugin.accessor.get_metric(
                _TEST_METRIC
            ).metadata.aggregator.carbon_name,
            "sum",
        )

        # Setting a different value should work
        self._plugin.setMetadata(_TEST_METRIC, "aggregationMethod", "avg")
        self.assertEqual(
            self._plugin.getMetadata(_TEST_METRIC, "aggregationMethod"), "avg"
        )
        self.assertEqual(
            self._plugin.accessor.get_metric(
                _TEST_METRIC
            ).metadata.aggregator.carbon_name,
            "avg",
        )
        self._plugin.setMetadata(_TEST_METRIC, "aggregationMethod", "sum")

        # Setting a name that is not in MetricMetadata.__slots__ should fail
        self.assertRaises(
            ValueError,
            self._plugin.setMetadata,
            _TEST_METRIC,
            "unsupportedMetadata",
            42,
        )

    def test_write(self):
        points = [(1, 42)]
        # Writing twice (the first write is sync and the next one isn't)
        self._plugin.write(_TEST_METRIC, points)
        self._plugin.write(_TEST_METRIC, points)
        self.accessor.flush()
        metric = self.accessor.get_metric(_TEST_METRIC)
        actual_points = self.accessor.fetch_points(
            metric, 1, 2, stage=metric.retention[0]
        )
        self.assertEqual(points, list(actual_points))

    def test_write_doubledots(self):
        metric = bg_test_utils.make_metric("a.b..c")
        metric_1 = bg_test_utils.make_metric("a.b.c")
        points = [(1, 42)]
        self.accessor.create_metric(metric)
        self._plugin.write(metric.name, points)
        self.accessor.flush()

        self.assertEqual(True, self.accessor.has_metric("a.b..c"))
        self.assertNotEqual(None, self.accessor.get_metric("a.b..c"))

        actual_points = self.accessor.fetch_points(
            metric, 1, 2, stage=metric.retention[0]
        )
        self.assertEqual(points, list(actual_points))
        actual_points = self.accessor.fetch_points(
            metric_1, 1, 2, stage=metric.retention[0]
        )
        self.assertEqual(points, list(actual_points))


class TestMultiDatabase(bg_test_utils.TestCaseWithFakeAccessor):
    def setUp(self):
        super(TestMultiDatabase, self).setUp()
        self.fake_drivers()
        settings = carbon_conf.Settings()
        settings["BG_CASSANDRA_CONTACT_POINTS"] = "host1,host2"
        settings["BG_CASSANDRA_KEYSPACE"] = self.KEYSPACE
        settings["STORAGE_DIR"] = self.tempdir
        settings["LOCAL_DATA_DIR"] = self.tempdir
        self._settings = settings

    def _test_plugin(self, klass):
        plugin = klass(self._settings)
        self.assertFalse(plugin.exists(_TEST_METRIC))
        plugin.create(
            _TEST_METRIC,
            retentions=[(1, 60)],
            xfilesfactor=0.5,
            aggregation_method="sum",
        )
        self.assertTrue(plugin.exists(_TEST_METRIC))
        plugin.write(_TEST_METRIC, [(1, 1), (2, 2)])
        self.assertEqual(plugin.getMetadata(_TEST_METRIC, "aggregationMethod"), "sum")

    def test_whisper_and_biggraphite(self):
        self._test_plugin(bg_carbon.WhisperAndBigGraphiteDatabase)

    def test_biggraphite_and_whisper(self):
        self._test_plugin(bg_carbon.BigGraphiteAndWhisperDatabase)

    def test_plugin_registration(self):
        plugins = database.TimeSeriesDatabase.plugins.keys()
        self.assertTrue("whisper+biggraphite" in plugins)
        self.assertTrue("biggraphite+whisper" in plugins)
        self.assertTrue("biggraphite" in plugins)


if __name__ == "__main__":
    unittest.main()
