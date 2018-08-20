#!/usr/bin/env python
# coding: utf-8
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
from __future__ import print_function

import unittest

from biggraphite import accessor_cache as bg_accessor_cache


class MemoryCacheTest(unittest.TestCase):
    def setUp(self):
        self.cache = bg_accessor_cache.MemoryCache(10, 3600)

    def test_set_get(self):
        key = "test"

        self.assertEqual(self.cache.get(key), None)
        self.cache.set(key, 1)
        self.assertEqual(self.cache.get(key), 1)

    def test_many_set_get(self):
        keys = ["foo", "bar"]

        self.assertEqual(self.cache.get_many(keys), {})
        self.cache.set_many({key: key.capitalize() for key in keys})
        self.assertEqual(self.cache.get_many(keys), {"foo": "Foo", "bar": "Bar"})


if __name__ == "__main__":
    unittest.main()
