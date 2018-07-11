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
# See the License for the specific lanbg_guage governing permissions and
# limitations under the License.

from __future__ import print_function

import unittest
import os

from biggraphite import utils as bg_utils


class TestGraphiteUtilsInternals(unittest.TestCase):

    def _check_settings(self, settings):
        # A non existing value.
        value, found = bg_utils.get_setting(settings, "BAR")
        self.assertEqual(value, None)
        self.assertFalse(found)

        # An existing value.
        value, found = bg_utils.get_setting(settings, "FOO")
        self.assertEqual(value, "BAR")
        self.assertTrue(found)

    def test_carbon_settings(self):
        from carbon import conf as carbon_conf
        settings = carbon_conf.Settings()
        settings["FOO"] = "BAR"
        self._check_settings(settings)

    def test_django_settings(self):
        import types
        settings = types.ModuleType("settings")
        settings.FOO = "BAR"
        self._check_settings(settings)

    def test_cassandra_accessor(self):
        settings = {"BG_DRIVER": "cassandra"}
        settings["BG_CASSANDRA_CONTACT_POINTS"] = "localhost"
        settings["BG_CASSANDRA_COMPRESSION"] = True
        settings["BG_CASSANDRA_TIMEOUT"] = 5

        settings = bg_utils.settings_from_confattr(settings)
        accessor = bg_utils.accessor_from_settings(settings)
        self.assertNotEqual(accessor, None)

        settings["BG_CASSANDRA_COMPRESSION"] = False
        settings = bg_utils.settings_from_confattr(settings)
        accessor = bg_utils.accessor_from_settings(settings)
        self.assertNotEqual(accessor, None)

    def test_memory_accessor(self):
        settings = {"BG_DRIVER": "memory"}
        settings = bg_utils.settings_from_confattr(settings)
        accessor = bg_utils.accessor_from_settings(settings)
        self.assertNotEqual(accessor, None)

    def test_set_log_level(self):
        bg_utils.set_log_level({"log_level": "INFO"})

    def test_manipulate_paths_like_upstream(self):
        sys_path = []
        bg_utils.manipulate_paths_like_upstream(
            "/a/b/c/bin/bg-carbon-aggregator-cache", sys_path)
        self.assertEqual(1, len(sys_path))
        self.assertEqual("/a/b/c/lib", sys_path[0])

    def test_setup_graphite_root_path(self):
        bg_utils.setup_graphite_root_path("/fake/path/to/carbon/file")
        assert "GRAPHITE_ROOT" not in os.environ
        bg_utils.setup_graphite_root_path("/opt/graphite/lib/carbon/file")
        assert "GRAPHITE_ROOT" in os.environ
        self.assertEqual(os.environ["GRAPHITE_ROOT"], '/opt/graphite')


if __name__ == "__main__":
    unittest.main()
