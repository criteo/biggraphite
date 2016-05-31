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
from __future__ import print_function

from biggraphite import test_utils as bg_test_utils   # noqa
bg_test_utils.prepare_graphite_imports()  # noqa

import unittest

from carbon import conf as carbon_conf
from carbon import exceptions as carbon_exceptions

from biggraphite.plugins import carbon as bg_carbon


_TEST_METRIC = "mytestmetric"


class TestCarbonDatabase(bg_test_utils.TestCaseWithFakeAccessor):

    def setUp(self):
        super(TestCarbonDatabase, self).setUp()
        self.patch_accessor()
        settings = carbon_conf.Settings()
        settings["BG_CONTACT_POINTS"] = "host1,host2"
        settings["BG_KEYSPACE"] = self.KEYSPACE
        self._plugin = bg_carbon.BigGraphiteDatabase(settings)
        self._plugin.create(
            _TEST_METRIC,
            retentions=[(1, 60)],
            xfilesfactor=0.5,
            aggregation_method="sum",
        )

    def test_empty_settings(self):
        self.assertRaises(carbon_exceptions.CarbonConfigException,
                          bg_carbon.BigGraphiteDatabase, carbon_conf.Settings())

    def test_get_fs_path(self):
        path = self._plugin.getFilesystemPath(_TEST_METRIC)
        self.assertTrue(path.startswith("//biggraphite/"))
        self.assertIn(_TEST_METRIC, path)
        self.assertIn(self.KEYSPACE, path)

    def test_create_get(self):
        other_metric = _TEST_METRIC + "-other"
        self._plugin.create(
            other_metric,
            retentions=[(1, 60)],
            xfilesfactor=0.5,
            aggregation_method="avg",
        )
        self.assertTrue(self._plugin.exists(other_metric))
        self.assertEqual("avg", self._plugin.getMetadata(other_metric, "aggregationMethod"))

    def test_nosuchmetric(self):
        other_metric = _TEST_METRIC + "-nosuchmetric"
        self.assertRaises(
            ValueError,
            self._plugin.setMetadata, other_metric, "aggregationMethod", "avg")
        self.assertRaises(
            ValueError,
            self._plugin.getMetadata, other_metric, "aggregationMethod")

    def test_set(self):
        # Setting the same value should work
        self._plugin.setMetadata(_TEST_METRIC, "aggregationMethod", "sum")
        # Setting a different value should fail
        self.assertRaises(
            ValueError,
            self._plugin.setMetadata, _TEST_METRIC, "aggregationMethod", "avg")


if __name__ == "__main__":
    unittest.main()
