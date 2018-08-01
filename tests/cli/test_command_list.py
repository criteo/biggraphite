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
import argparse

from biggraphite.cli import command_list
from biggraphite import settings as bg_settings
from biggraphite import accessor as bg_accessor
from tests import test_utils as bg_test_utils


class TestCommandList(bg_test_utils.TestCaseWithFakeAccessor):

    def test_run(self):
        self.accessor.drop_all_metrics()

        cmd = command_list.CommandList()

        parser = argparse.ArgumentParser()
        bg_settings.add_argparse_arguments(parser)
        cmd.add_arguments(parser)

        name = 'foo.bar'
        metadata = bg_accessor.MetricMetadata(
            retention=bg_accessor.Retention.from_string('1440*60s'))

        self.accessor.create_metric(bg_test_utils.make_metric(name, metadata))

        opts = parser.parse_args(['foo.*'])
        cmd.run(self.accessor, opts)
        metrics = command_list.list_metrics(
            self.accessor, opts.glob, opts.graphite)
        self.assertEqual(name, list(metrics)[0].name)

        opts = parser.parse_args(['--graphite', 'foo.{bar}'])
        metrics = command_list.list_metrics(
            self.accessor, opts.glob, opts.graphite)
        self.assertEqual(name, list(metrics)[0].name)


if __name__ == "__main__":
    unittest.main()
