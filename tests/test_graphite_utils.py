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

from biggraphite import graphite_utils as bg_graphite_utils


class TestGraphiteUtils(unittest.TestCase):
    def test_accessor_from_settings(self):
        import types

        settings = types.ModuleType("settings")
        settings.BG_DRIVER = "memory"
        accessor = bg_graphite_utils.accessor_from_settings(settings)
        self.assertNotEqual(accessor, None)

    def test_cache_from_settings(self):
        import types

        settings = types.ModuleType("settings")
        settings.BG_CACHE = "memory"
        settings.BG_CACHE_SIZE = 10
        settings.BG_CACHE_TTL = 60
        settings.BG_CACHE_SYNC = False
        cache = bg_graphite_utils.cache_from_settings("fake", settings)
        self.assertNotEqual(cache, None)


if __name__ == "__main__":
    unittest.main()
