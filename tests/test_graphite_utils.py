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

from biggraphite import test_utils as bg_test_utils
from biggraphite import graphite_utils as bg_gu


class TestGraphiteUtilsInternals(unittest.TestCase):

    def test_storage_path_from_settings(self):
        settings = {}
        self.assertRaises(bg_gu.ConfigError,
                          bg_gu.storage_path, settings)

        settings["STORAGE_DIR"] = "/"
        self.assertEquals("/", bg_gu.storage_path(settings))

    def test_is_graphite_glob(self):
        self.assertTrue(bg_gu._is_graphite_glob("a*"))
        self.assertTrue(bg_gu._is_graphite_glob("a.b*"))
        self.assertTrue(bg_gu._is_graphite_glob("a.b?"))
        self.assertTrue(bg_gu._is_graphite_glob("a.b[a-z]?"))
        self.assertTrue(bg_gu._is_graphite_glob("a{b,c,d}.a"))
        self.assertTrue(bg_gu._is_graphite_glob("{b}"))
        self.assertTrue(bg_gu._is_graphite_glob("a.*.a"))
        self.assertFalse(bg_gu._is_graphite_glob("a.a"))
        self.assertFalse(bg_gu._is_graphite_glob("a-z"))

    def test_graphite_glob_to_accessor_components(self):
        self.assertEqual('a.*.b', bg_gu._graphite_glob_to_accessor_components('a.*.b'))
        self.assertEqual('a.*.b', bg_gu._graphite_glob_to_accessor_components('a.?.b'))
        self.assertEqual('*.b', bg_gu._graphite_glob_to_accessor_components('a?.b'))
        self.assertEqual('*.b', bg_gu._graphite_glob_to_accessor_components('a{x,y}.b'))
        self.assertEqual('*.b', bg_gu._graphite_glob_to_accessor_components('a{x,y}z.b'))
        self.assertEqual('*.b', bg_gu._graphite_glob_to_accessor_components('a[0-9].b'))
        self.assertEqual('*.b', bg_gu._graphite_glob_to_accessor_components('a[0-9]z.b'))
        self.assertEqual('*.*', bg_gu._graphite_glob_to_accessor_components('a[0-9]z.{b}'))

    def test_filter_metrics(self):
        # pylama:ignore=E501
        self.assertEqual(['a.b', 'a.cc'], bg_gu._filter_metrics(['a', 'a.b', 'a.cc'], 'a.*'))
        self.assertEqual(['a.b'], bg_gu._filter_metrics(['a.b', 'a.cc'], 'a.?'))
        self.assertEqual(['a.b', 'a.cc', 'y.z'], bg_gu._filter_metrics(['y.z', 'a.b', 'a.cc'], '?.*'))
        self.assertEqual(['a.bd', 'a.cd'], bg_gu._filter_metrics(['y.z', 'a.bd', 'a.cd'], '?.{b,c}?'))
        self.assertEqual(['a.0_', 'a.1_'], bg_gu._filter_metrics(['a.b_', 'a.0_', 'a.1_'], '?.[0-9]?'))
        self.assertEqual(['a.b.c', 'a.x.y'], bg_gu._filter_metrics(['a.b', 'a.b.c', 'a.x.y'], 'a.*.*'))
        self.assertEqual(['a.b.c', 'a.x.y'], bg_gu._filter_metrics(['a.b', 'a.b.c', 'a.x.y'], 'a.{b,x}.*'))
        self.assertEqual(['a.b.c', 'a.x.y'], bg_gu._filter_metrics(['a.b', 'a.b.c', 'a.x.y'], 'a.{b,x}.{c,y}'))


class TestGraphiteUtils(bg_test_utils.TestCaseWithFakeAccessor):

    def test_glob(self):
        for name in "a", "a.b.c", "a.b.d", "x.y.c", "a.a.a":
            metric = bg_test_utils.make_metric(name)
            self.accessor.create_metric(metric)
        self.assertEqual((["a"], ["a", "x"]), bg_gu.glob(self.accessor, "*"))
        self.assertEqual((["a.b.c", "x.y.c"], []), bg_gu.glob(self.accessor, "*.*.c"))
        self.assertEqual((["a.a.a", "a.b.c", "a.b.d"], []), bg_gu.glob(self.accessor, "a.*.*"))
        self.assertEqual((["a.a.a", "a.b.c", "a.b.d", "x.y.c"], []), bg_gu.glob(self.accessor, "*.*.*"))
        self.assertEqual((["a.b.c", "a.b.d"], []), bg_gu.glob(self.accessor, "*.{b,c,d,5}.?"))

    def test_storage_path(self):
        import types
        settings = types.ModuleType("settings")
        settings.STORAGE_DIR = "/tmp"
        storage_path = bg_gu.storage_path(settings)
        self.assertEquals(storage_path, settings.STORAGE_DIR)

    def test_accessor_from_settings(self):
        import types
        settings = types.ModuleType("settings")
        settings.BG_DRIVER = "memory"
        accessor = bg_gu.accessor_from_settings(settings)
        self.assertNotEquals(accessor, None)


if __name__ == "__main__":
    unittest.main()
