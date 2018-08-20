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
import unittest

from biggraphite import metric as bg_metric
from tests import test_utils as bg_test_utils


class TestMetric(unittest.TestCase):
    def test_dir(self):
        metric = bg_test_utils.make_metric("a.b.c")
        self.assertIn("name", dir(metric))
        self.assertIn("carbon_xfilesfactor", dir(metric))


class TestAccessorFunctions(unittest.TestCase):
    def test_sanitize_metric_name_should_remove_multiple_dots(self):
        self.assertEqual("foo.bar.baz", bg_metric.sanitize_metric_name("foo.bar..baz"))
        self.assertEqual("foo.bar.baz", bg_metric.sanitize_metric_name("foo.bar...baz"))
        self.assertEqual("foo.bar.baz", bg_metric.sanitize_metric_name("foo..bar..baz"))

    def test_sanitize_metric_name_should_trim_trailing_dots(self):
        self.assertEqual("foo.bar", bg_metric.sanitize_metric_name("foo.bar."))
        self.assertEqual("foo.bar", bg_metric.sanitize_metric_name("foo.bar.."))

    def test_sanitize_metric_name_should_trim_heading_dots(self):
        self.assertEqual("foo.bar", bg_metric.sanitize_metric_name(".foo.bar"))
        self.assertEqual("foo.bar", bg_metric.sanitize_metric_name("..foo.bar"))

    def test_sanitize_metric_name_should_handle_None_value(self):
        self.assertEqual(None, bg_metric.sanitize_metric_name(None))

    def test_sanitize_metric_name_should_handle_empty_value(self):
        self.assertEqual("", bg_metric.sanitize_metric_name(""))
        self.assertEqual("", bg_metric.sanitize_metric_name("."))
        self.assertEqual("", bg_metric.sanitize_metric_name(".."))
