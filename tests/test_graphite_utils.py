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

from biggraphite import graphite_utils as bg_graphite_utils


class TestGraphiteUtilsInternals(unittest.TestCase):

    def test_storage_path_from_settings(self):
        settings = {}
        self.assertRaises(bg_graphite_utils.ConfigError,
                          bg_graphite_utils.storage_path, settings)

        settings["STORAGE_DIR"] = "/"
        self.assertEquals("/", bg_graphite_utils.storage_path(settings))


class TestGraphiteUtils(unittest.TestCase):

    def test_storage_path(self):
        import types
        settings = types.ModuleType("settings")
        settings.STORAGE_DIR = "/tmp"
        storage_path = bg_graphite_utils.storage_path(settings)
        self.assertEquals(storage_path, settings.STORAGE_DIR)

    def test_accessor_from_settings(self):
        import types
        settings = types.ModuleType("settings")
        settings.BG_DRIVER = "memory"
        accessor = bg_graphite_utils.accessor_from_settings(settings)
        self.assertNotEquals(accessor, None)


if __name__ == "__main__":
    unittest.main()
