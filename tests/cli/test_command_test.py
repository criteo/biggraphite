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

import argparse
import unittest

from biggraphite import settings as bg_settings
from biggraphite.cli import command_test
from tests import test_utils as bg_test_utils


class TestCommandTest(bg_test_utils.TestCaseWithFakeAccessor):
    def test_run(self):
        cmd = command_test.CommandTest()

        parser = argparse.ArgumentParser()
        bg_settings.add_argparse_arguments(parser)
        cmd.add_arguments(parser)
        opts = parser.parse_args([])
        cmd.run(self.accessor, opts)

    def test_run_with_args(self):
        cmd = command_test.CommandTest()

        parser = argparse.ArgumentParser()
        bg_settings.add_argparse_arguments(parser)
        cmd.add_arguments(parser)
        opts = parser.parse_args(
            [
                "--cassandra_contact_points=127.0.0.1,192.168.0.1",
                "--cassandra_contact_points_metadata=127.0.0.1,192.168.1.1",
            ]
        )
        settings = bg_settings.settings_from_args(opts)
        self.assertIsInstance(settings["cassandra_contact_points"], list)
        self.assertIsInstance(settings["cassandra_contact_points_metadata"], list)

        cmd.run(self.accessor, opts)


if __name__ == "__main__":
    unittest.main()
