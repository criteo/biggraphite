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

import re
import unittest
from collections import defaultdict

import biggraphite.metric as bg_metric
from biggraphite import glob_utils as bg_glob
from tests import test_utils as bg_test_utils


class TestGlobUtilsInternals(unittest.TestCase):
    def test_is_graphite_glob(self):
        is_glob = ["a*", "a.b*", "a.b?", "a.b[a-z]?", "a{b,c,d}.a", "a.*.a", "{a}"]
        for x in is_glob:
            self.assertTrue(bg_glob._is_graphite_glob(x))

        not_glob = ["a.a", "a-z"]
        for x in not_glob:
            self.assertFalse(bg_glob._is_graphite_glob(x))

    def test_is_valid_glob(self):
        valid_glob = ["a", "a.b", "{a}.b", "{a,{b,c}}.d", "{a,b}.{c,d}.e"]
        for x in valid_glob:
            self.assertTrue(bg_glob._is_valid_glob(x))

        invalid_glob = [
            "{",
            "{{}",
            "{}}",
            "}{",
            "}{}",
            "{a.}.b",
            "{a,{.b,c}}.d",
            "{a,b.}.{.c,d}.e",
        ]
        for x in invalid_glob:
            self.assertFalse(bg_glob._is_valid_glob(x))

    def test_glob_to_regex(self):
        def filter_metrics(metrics, glob):
            print(glob + "   ===>   " + bg_glob.glob_to_regex(glob))
            glob_re = re.compile(bg_glob.glob_to_regex(glob))
            return list(filter(glob_re.match, metrics))

        scenarii = [
            (["a", "a.b", "a.cc"], "a.*", ["a.b", "a.cc"]),
            (["a.b", "a.cc"], "a.?", ["a.b"]),
            (["a.b", "a.cc", "y.z"], "?.*", ["a.b", "a.cc", "y.z"]),
            (["a.bd", "a.cd", "y.z"], "?.{b,c}?", ["a.bd", "a.cd"]),
            (["a.b_", "a.0_", "a.1_"], "?.[0-9]?", ["a.0_", "a.1_"]),
            (["a.b", "a.b.c", "a.x.y"], "a.*.*", ["a.b.c", "a.x.y"]),
            (["a.b", "a.b.c", "a.x.y"], "a.{b,x}.*", ["a.b.c", "a.x.y"]),
            (["a.b", "a.b.c", "a.x.y"], "a.{b,x}.{c,y}", ["a.b.c", "a.x.y"]),
            (
                ["a.b", "a.b.c", "a.x.y", "a.x.z"],
                "a.{b,x}.{c,{y,z}}",
                ["a.b.c", "a.x.y", "a.x.z"],
            ),
            # issue 240
            (
                [
                    "fib.bar",
                    "fib.bart",
                    "foo.baaa",
                    "foo.bar",
                    "foo.bart",
                    "foo.bli",
                    "foo.blo",
                ],
                "foo.{bar*,bli}",
                ["foo.bar", "foo.bart", "foo.bli"],
            ),
            # issue 290
            (
                [
                    "fib.bar.la",
                    "fib.bart.la",
                    "foo.baaa.la",
                    "foo.bar.la",
                    "foo.bart.la",
                    "foo.blit.la",
                    "foo.blo.la",
                ],
                "foo.{bar*,bli*}.la",
                ["foo.bar.la", "foo.bart.la", "foo.blit.la"],
            ),
        ]
        for (full, glob, filtered) in scenarii:
            self.assertEqual(filtered, filter_metrics(full, glob))

    def test_graphite_glob_parser(self):
        scenarii = [
            # Positive examples
            ("a.b", [["a"], ["b"]], True),
            ("a.{b}", [["a"], ["b"]], True),
            ("a?b.c", [["a", bg_glob.AnyChar(), "b"], ["c"]], False),
            ("a.b*c", [["a"], ["b", bg_glob.AnySequence(), "c"]], False),
            ("a.b**c", [["a"], ["b"], bg_glob.Globstar(), ["c"]], False),
            ("a.**.c", [["a"], bg_glob.Globstar(), ["c"]], False),
            ("a.**", [["a"], bg_glob.Globstar()], False),
            ("a[xyz].b", [["a", bg_glob.CharIn(["x", "y", "z"])], ["b"]], False),
            ("a[!rat].b", [["a", bg_glob.CharNotIn(["r", "a", "t"])], ["b"]], False),
            ("pl[a-ox]p", [["pl", bg_glob.CharIn(["a-o", "x"]), "p"]], False),
            (
                "a[b-dopx-z]b.c",
                [["a", bg_glob.CharIn(["b-d", "o", "p", "x-z"]), "b"], ["c"]],
                False,
            ),
            ("b[i\\]m", [["b", bg_glob.CharIn(["\\", "i"]), "m"]], False),
            ("a[x-xy]b", [["a", bg_glob.CharIn(["x-x", "y"]), "b"]], False),
            ("a[y-xz]b", [["a", bg_glob.CharIn(["y-x", "z"]), "b"]], False),
            ("a.b.{c,d}", [["a"], ["b"], [bg_glob.SequenceIn(["c", "d"])]], False),
            (
                "a.b.{c,d}-{e,f}",
                [
                    ["a"],
                    ["b"],
                    [
                        bg_glob.SequenceIn(["c", "d"]),
                        "-",
                        bg_glob.SequenceIn(["e", "f"]),
                    ],
                ],
                False,
            ),
            (
                "a.b.oh{c{d,e,}{a,b},f{g,h}i}ah",
                [
                    ["a"],
                    ["b"],
                    [
                        "oh",
                        bg_glob.SequenceIn(
                            ["ca", "cb", "cda", "cdb", "cea", "ceb", "fgi", "fhi"]
                        ),
                        "ah",
                    ],
                ],
                False,
            ),
            (
                "a.b{some, x{chars[!xyz], plop}}c",
                [["a"], ["b", bg_glob.AnySequence(), "c"]],
                False,
            ),
            # Negative examples
            ("a[.b", [["a["], ["b"]], True),
            ("a{.b", [["a{"], ["b"]], True),
            ("a{.b.c}", [["a{"], ["b"], ["c}"]], True),
            ("a.", [["a"]], True),
            ("a..b", [["a"], ["b"]], True),
        ]
        parser = bg_glob.GraphiteGlobParser()
        for i, (glob, expected, fully_defined) in enumerate(scenarii):
            parsed = parser.parse(glob)
            self.assertSequenceEqual(expected, parsed)
            self.assertEqual(fully_defined, parser.is_fully_defined(parsed), parsed)

    def test_filter_from_glob_should_keep_exact_match(self):
        name = "foo.bar.baz"
        filtered = bg_glob.filter_from_glob([name], name)
        self.assertEqual([name], filtered)

    def test_filter_from_glob_should_keep_regexp_match(self):
        regexp = "foo.bar.*"
        matching_name_1 = "foo.bar.baz"
        matching_name_2 = "foo.bar.qux"

        filtered = bg_glob.filter_from_glob([matching_name_1, matching_name_2], regexp)

        self.assertEqual([matching_name_1, matching_name_2], filtered)

    def test_filter_from_glob_should_remove_regexp_match(self):
        regexp = "foo.bar.*"
        matching_name = "foo.bar.baz"
        not_matching_name_1 = "bar.foo.qux"
        not_matching_name_2 = "bar.foo.quux"

        filtered = bg_glob.filter_from_glob(
            [matching_name, not_matching_name_1, not_matching_name_2], regexp
        )

        self.assertEqual([matching_name], filtered)


class TestGlobUtils(bg_test_utils.TestCaseWithFakeAccessor):
    _metric_names = sorted(["a", "aa", "aaa", "b", "a.a.a", "a.b.c", "a.b.d", "x.y.c"])

    _metrics_by_length = defaultdict(list)
    for metric in _metric_names:
        _metrics_by_length[metric.count(".") + 1].append(metric)

    def test_glob(self):
        scenarii = [
            # Single character wildcard
            ("a?", self._metrics_by_length[1]),
            # Component wildcard
            ("*", self._metrics_by_length[1]),
            ("*.*.c", ["a.b.c", "x.y.c"]),
            ("a.*.*", ["a.a.a", "a.b.c", "a.b.d"]),
            ("*.*.*", self._metrics_by_length[3]),
            # Zero-or-more characters wildcard
            ("a*", self._metrics_by_length[1]),
            # Choices
            ("*.{b,c,d,5}.?", ["a.a.a", "a.b.c", "a.b.d", "x.y.c"]),
            # Globstar wildcard
            ("a.**", ["a.a.a", "a.b.c", "a.b.d"]),
        ]
        for (glob, metrics) in scenarii:
            found = bg_glob.glob(self._metric_names, glob)
            self.assertEqual(metrics, found)

    def test_graphite_glob(self):
        for name in self._metric_names:
            metric = bg_test_utils.make_metric(name)
            self.accessor.create_metric(metric)

        scenarii = [
            # Single character wildcard
            ("a?", ["aa"], []),
            # Component wildcard
            ("*", self._metrics_by_length[1], ["a", "x"]),
            ("*.*.c", ["a.b.c", "x.y.c"], []),
            ("a.*.*", ["a.a.a", "a.b.c", "a.b.d"], []),
            ("*.*.*", self._metrics_by_length[3], []),
            # Multi-character wildcard
            ("a*", ["a", "aa", "aaa"], ["a"]),
            # Choices
            ("*.{b,c,d,5}.?", ["a.b.c", "a.b.d"], []),
            # Globstar wildcard
            ("a.**", ["a.a.a", "a.b.c", "a.b.d"], ["a.a", "a.b"]),
        ]
        for (glob, metrics, directories) in scenarii:
            (found_metrics, found_directories) = bg_glob.graphite_glob(self.accessor, glob)
            found_metric_names = [found_metric.name for found_metric in found_metrics]
            found_directory_names = [found_directory.name for found_directory in found_directories]
            self.assertEqual((metrics, directories), (found_metric_names, found_directory_names))


class TestGraphiteGlobResult(unittest.TestCase):

    def test_GlobMetricResult_be_built_from_a_metric(self):
        metric = bg_metric.Metric(name="foo.bar", id="0", metadata=bg_metric.MetricMetadata())

        metric_result = bg_glob.GlobMetricResult.from_value(metric)

        self.assertEqual(metric_result.value, metric)

    def test_GlobMetricResult_built_from_a_metric_should_discover_its_name(self):
        metric_name = "foo.bar"
        metric = bg_metric.Metric(name=metric_name, id="0", metadata=bg_metric.MetricMetadata())

        metric_result = bg_glob.GlobMetricResult.from_value(metric)

        self.assertEqual(metric_result.name, metric_name)

    def test_GlobMetricResult_from_value_should_raise_if_value_is_not_a_metric(self):
        with self.assertRaises(AssertionError):
            bg_glob.GlobMetricResult.from_value("foo")


if __name__ == "__main__":
    unittest.main()
