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

from mock import patch
from six import StringIO

from biggraphite import settings as bg_settings
from biggraphite.cli import command_stats
from biggraphite.metric import MetricMetadata
from biggraphite.metric import Retention
from tests import test_utils as bg_test_utils


class TestCommandStats(bg_test_utils.TestCaseWithFakeAccessor):

    metrics = ["metric1", "metric2"]
    metadata = MetricMetadata(retention=Retention.from_string("1440*60s"))

    @patch("sys.stdout", new_callable=StringIO)
    def get_output(self, args, mock_stdout):
        self.accessor.drop_all_metrics()
        for metric in self.metrics:
            self.accessor.create_metric(
                bg_test_utils.make_metric(metric, self.metadata)
            )

        cmd = command_stats.CommandStats()

        parser = argparse.ArgumentParser(add_help=False)
        bg_settings.add_argparse_arguments(parser)
        cmd.add_arguments(parser)

        opts = parser.parse_args(args)
        cmd.run(self.accessor, opts)
        return mock_stdout.getvalue()

    def test_simple(self):
        output = self.get_output([])
        self.assertIn("Namespace      Metrics    Points", output)
        self.assertIn("2      2880", output)

    def test_graphite(self):
        output = self.get_output(["--format", "graphite"])
        self.assertIn("metrics.total 2", output)
        self.assertIn("points.total 2880", output)


if __name__ == "__main__":
    unittest.main()
