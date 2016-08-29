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
# See the License for the specific lanbg_guage governing permissions and
# limitations under the License.

from __future__ import print_function

import unittest

from biggraphite import utils as bg_utils


class TestGraphiteUtilsInternals(unittest.TestCase):

    def test_storage_path_from_settings(self):
        settings = {}
        self.assertRaises(bg_utils.ConfigError,
                          bg_utils.storage_path_from_settings, settings)

        settings["STORAGE_DIR"] = "/"
        self.assertEquals("/", bg_utils.storage_path_from_settings(settings))

    def _check_settings(self, settings):
        # A non existing non-optional value with no default.
        self.assertRaises(bg_utils.ConfigError,
                          bg_utils._get_setting, settings, "BAR")

        # A non existing optional value with a default.
        value = bg_utils._get_setting(settings, "BAR", optional=True)
        self.assertEquals(value, None)

        # A non existing value with a default.
        value = bg_utils._get_setting(settings, "BAR", default="FOO")
        self.assertEquals(value, "FOO")

        # An existing value.
        value = bg_utils._get_setting(settings, "FOO")
        self.assertEquals(value, "BAR")

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
        settings = {'BG_DRIVER': 'cassandra'}
        settings["BG_CASSANDRA_CONTACT_POINTS"] = "localhost"
        accessor = bg_utils.accessor_from_settings(settings)
        self.assertNotEquals(accessor, None)

    def test_memory_accessor(self):
        settings = {'BG_DRIVER': 'memory'}
        accessor = bg_utils.accessor_from_settings(settings)
        self.assertNotEquals(accessor, None)


if __name__ == "__main__":
    unittest.main()
