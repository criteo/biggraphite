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
"""Tests for Capture."""

import logging
import sys
from unittest import TestCase

from biggraphite.cli.web.capture import Capture


class TestCapture(TestCase):
    def test_capture_should_get_stdout_content_from_outside_context(self):
        content = "foo"
        with Capture() as capture:
            sys.stdout.write(content)
        self.assertEqual(capture.get_content(), content)

    def test_capture_should_get_stdout_content_from_inside_context(self):
        content = "foo"
        with Capture() as capture:
            sys.stdout.write(content)
            self.assertEqual(capture.get_content(), content)

    def test_capture_should_get_stderr_content_from_outside_context(self):
        content = "foo"
        with Capture() as capture:
            sys.stderr.write(content)
        self.assertEqual(capture.get_content(), content)

    def test_capture_should_get_stderr_content_from_inside_context(self):
        content = "foo"
        with Capture() as capture:
            sys.stderr.write(content)
            self.assertEqual(capture.get_content(), content)

    def test_capture_should_get_stdout_content_only_from_context(self):
        unexpected_content = "foo"
        expected_content = "bar"
        sys.stdout.write(unexpected_content)
        with Capture() as capture:
            sys.stdout.write(expected_content)
        self.assertEqual(capture.get_content(), expected_content)

    def test_capture_should_get_logger_content_with_line_break(self):
        content = "foo"
        logger = logging.getLogger("test-logger")
        with Capture() as capture:
            logger.info(content)
        self.assertEqual(capture.get_content(), content + "\n")

    def test_capture_should_get_print_content(self):
        content = "Hello"
        with Capture() as capture:
            print(content)
        self.assertEqual(capture.get_content(), content + "\n")

    def test_capture_should_handle_line_breaks(self):
        content_line_1 = "Hello"
        content_line_2 = "World"
        expected_result = "%s\n%s\n" % (content_line_1, content_line_2)
        with Capture() as capture:
            print(content_line_1)
            print(content_line_2)
        self.assertEqual(capture.get_content(), expected_result)
