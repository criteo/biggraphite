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

import re
import unittest

from biggraphite import test_utils as bg_test_utils
from biggraphite import glob_utils as bg_glob


class TestGlobUtilsInternals(unittest.TestCase):

    def test_is_graphite_glob(self):
        is_glob = [
            "a*",
            "a.b*",
            "a.b?",
            "a.b[a-z]?",
            "a{b,c,d}.a",
            "a.*.a",
            "{a}",
        ]
        for x in is_glob:
            self.assertTrue(bg_glob._is_graphite_glob(x))

        not_glob = [
            "a.a",
            "a-z",
        ]
        for x in not_glob:
            self.assertFalse(bg_glob._is_graphite_glob(x))

    def test_is_valid_glob(self):
        valid_glob = [
            "a",
            "a.b",
            "{a}.b",
            "{a,{b,c}}.d",
            "{a,b}.{c,d}.e",
        ]
        for x in valid_glob:
            self.assertTrue(bg_glob._is_valid_glob(x))

        invalid_glob = [
            "{", "{{}", "{}}", "}{", "}{}",
            "{a.}.b",
            "{a,{.b,c}}.d",
            "{a,b.}.{.c,d}.e",
        ]
        for x in invalid_glob:
            self.assertFalse(bg_glob._is_valid_glob(x))

    def test_graphite_glob_to_accessor_components(self):
        scenarii = [
            ('a.*.b', 'a.*.b'),
            ('a.?.b', 'a.*.b'),
            ('a?.b',      '*.b'),
            ('a{x,y}.b',  '*.b'),
            ('a{x,y}z.b', '*.b'),
            ('a[0-9].b',  '*.b'),
            ('a[0-9]z.b', '*.b'),
        ]
        for (glob, components) in scenarii:
            self.assertEqual(components, bg_glob._graphite_glob_to_accessor_components(glob))

    def test_glob_to_regex(self):
        def filter_metrics(metrics, glob):
            glob_re = re.compile(bg_glob._glob_to_regex(glob))
            return filter(glob_re.match, metrics)

        scenarii = [
            (['a', 'a.b', 'a.cc'], 'a.*', ['a.b', 'a.cc']),
            (['a.b', 'a.cc'], 'a.?', ['a.b']),
            (['a.b', 'a.cc', 'y.z'], '?.*', ['a.b', 'a.cc', 'y.z']),
            (['a.bd', 'a.cd', 'y.z'], '?.{b,c}?', ['a.bd', 'a.cd']),
            (['a.b_', 'a.0_', 'a.1_'], '?.[0-9]?', ['a.0_', 'a.1_']),
            (['a.b', 'a.b.c', 'a.x.y'], 'a.*.*', ['a.b.c', 'a.x.y']),
            (['a.b', 'a.b.c', 'a.x.y'], 'a.{b,x}.*', ['a.b.c', 'a.x.y']),
            (['a.b', 'a.b.c', 'a.x.y'], 'a.{b,x}.{c,y}', ['a.b.c', 'a.x.y']),
            (['a.b', 'a.b.c', 'a.x.y', 'a.x.z'], 'a.{b,x}.{c,{y,z}}', ['a.b.c', 'a.x.y', 'a.x.z']),
        ]
        for (full, glob, filtered) in scenarii:
            self.assertEqual(filtered, filter_metrics(full, glob))


class TestGlobUtils(bg_test_utils.TestCaseWithFakeAccessor):

    def test_glob(self):
        for name in "a", "a.a.a", "a.b.c", "a.b.d", "x.y.c":
            metric = bg_test_utils.make_metric(name)
            self.accessor.create_metric(metric)

        scenarii = [
            ("*", ["a"], ["a", "x"]),
            ("*.*.c", ["a.b.c", "x.y.c"], []),
            ("a.*.*", ["a.a.a", "a.b.c", "a.b.d"], []),
            ("*.*.*", ["a.a.a", "a.b.c", "a.b.d", "x.y.c"], []),
            ("*.{b,c,d,5}.?", ["a.b.c", "a.b.d"], []),
        ]
        for (glob, metrics, directories) in scenarii:
            self.assertEqual((metrics, directories), bg_glob.glob(self.accessor, glob))

if __name__ == "__main__":
    unittest.main()
