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

import unittest
import time
import re

from distutils import version

from biggraphite import accessor as bg_accessor
from biggraphite import accessor_cache as bg_accessor_cache
from biggraphite import test_utils as bg_test_utils
from biggraphite import glob_utils as bg_glob_utils
from biggraphite.drivers import elasticsearch as bg_elasticsearch


class ElasticSearchAccessorTest(unittest.TestCase):

    def test_components_from_name_should_create_array_by_splitting_on_dots(self):
        expected = ['a', 'b', 'c', 'd']
        result = bg_elasticsearch._components_from_name("a.b.c.d")
        self.assertEqual(result, expected)

    def test_components_from_name_should_ignore_double_dots(self):
        expected = ['a', 'b', 'c']
        result = bg_elasticsearch._components_from_name("a.b..c")
        self.assertEqual(result, expected)

    def test_components_from_name_should_handle_empty_strings(self):
        expected = []
        result = bg_elasticsearch._components_from_name("")
        self.assertEqual(result, expected)

    def test_document_from_metric_should_fail_on_None_metric(self):
        with self.assertRaises(AttributeError) as context:
            bg_elasticsearch.document_from_metric(None)
        self.assertTrue("object has no attribute" in context.exception.message)
