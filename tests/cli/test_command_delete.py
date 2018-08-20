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

from biggraphite import metric as bg_metric
from biggraphite import settings as bg_setting
from biggraphite.cli import command_delete
from tests import test_utils as bg_test_utils


class TestCommandDelete(bg_test_utils.TestCaseWithFakeAccessor):
    def test_run(self):
        self.accessor.drop_all_metrics()

        cmd = command_delete.CommandDelete()

        parser = argparse.ArgumentParser()
        bg_setting.add_argparse_arguments(parser)
        cmd.add_arguments(parser)

        name = "foo.bar"
        metadata = bg_metric.MetricMetadata(
            retention=bg_metric.Retention.from_string("1440*60s")
        )

        self.accessor.create_metric(bg_test_utils.make_metric(name, metadata))
        opts = parser.parse_args(["foo", "--recursive", "--dry-run"])
        cmd.run(self.accessor, opts)
        self.assertIn(name, self.accessor.glob_metric_names("*.*"))

        opts = parser.parse_args(["foo", "--recursive"])
        cmd.run(self.accessor, opts)
        self.assertNotIn(name, self.accessor.glob_metric_names("*.*"))


if __name__ == "__main__":
    unittest.main()
