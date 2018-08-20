# coding=utf-8
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
"""Tests for bgutil web."""

import unittest

import biggraphite.cli.web.namespaces.bgutil as bg_web


class TestBgUtil(unittest.TestCase):
    def test_parse_command_should_raise_UnknownCommandException_for_unknown_command(
        self
    ):
        with self.assertRaises(bg_web.UnknownCommandException):
            bg_web.parse_command("foo", {})
