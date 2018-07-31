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

from biggraphite.cli import command_graphite_web
from biggraphite import utils as bg_utils
from biggraphite import accessor as bg_accessor
from tests import test_utils as bg_test_utils


class TestCommandGraphiteWeb(bg_test_utils.TestCaseWithFakeAccessor):

    def test_run(self):
        name = 'foo.bar'
        metadata = bg_accessor.MetricMetadata(
            retention=bg_accessor.Retention.from_string('1440*60s'))
        self.accessor.create_metric(bg_test_utils.make_metric(name, metadata))
        self.accessor.flush()

        cmd = command_graphite_web.CommandGraphiteWeb()

        parser = argparse.ArgumentParser()
        bg_utils.add_argparse_arguments(parser)
        cmd.add_arguments(parser)
        opts = parser.parse_args(['foo.bar'])
        cmd.run(self.accessor, opts)


if __name__ == "__main__":
    unittest.main()
