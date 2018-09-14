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
import mock

from biggraphite import accessor_factory as bg_accessor_factory
from biggraphite import settings as bg_settings


class TestAccessorFactory(unittest.TestCase):
    def test_cassandra_accessor(self):
        settings = {"BG_DRIVER": "cassandra"}
        settings["BG_CASSANDRA_CONTACT_POINTS"] = "localhost"
        settings["BG_CASSANDRA_COMPRESSION"] = True
        settings["BG_CASSANDRA_TIMEOUT"] = 5

        settings = bg_settings.settings_from_confattr(settings)
        accessor = bg_accessor_factory.accessor_from_settings(settings)
        self.assertNotEqual(accessor, None)

        settings["BG_CASSANDRA_COMPRESSION"] = False
        settings = bg_settings.settings_from_confattr(settings)
        accessor = bg_accessor_factory.accessor_from_settings(settings)
        self.assertNotEqual(accessor, None)

    def test_memory_accessor(self):
        settings = {"BG_DRIVER": "memory"}
        settings = bg_settings.settings_from_confattr(settings)
        accessor = bg_accessor_factory.accessor_from_settings(settings)
        self.assertNotEqual(accessor, None)

    @mock.patch('biggraphite.drivers.cassandra._CassandraAccessor')
    def test_hybrid_accessor(self, cassandra_accessor_mock):
        settings = {
                    "BG_DATA_DRIVER": "cassandra",
                    "BG_METADATA_DRIVER": "memory"
                }
        settings = bg_settings.settings_from_confattr(settings)
        accessor = bg_accessor_factory.accessor_from_settings(settings)
        self.assertNotEqual(accessor, None)
        cassandra_accessor_mock.assert_called_with(enable_metadata=False)


if __name__ == "__main__":
    unittest.main()
