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
from __future__ import print_function

import unittest


# pylama:ignore=W0611
# No tests, at least check that the syntax is valid.
# TODO: bundle a small pcap file and test that we can parse
# it (also add a --dry_run).


class TestReplayTraffic(unittest.TestCase):
    def test_import(self):
        pass


if __name__ == "__main__":
    unittest.main()
