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

from biggraphite import settings as bg_settings


class TestGraphiteUtilsInternals(unittest.TestCase):
    def _check_settings(self, settings):
        # A non existing value.
        value, found = bg_settings.get_setting(settings, "BAR")
        self.assertEqual(value, None)
        self.assertFalse(found)

        # An existing value.
        value, found = bg_settings.get_setting(settings, "FOO")
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


if __name__ == "__main__":
    unittest.main()
