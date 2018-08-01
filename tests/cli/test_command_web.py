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

import prometheus_client

try:
    import gourde
except ImportError:
    gourde = None

from biggraphite.cli import command_web
from biggraphite import settings as bg_settings
from tests import test_utils as bg_test_utils


@unittest.skipUnless(gourde, "Gourde is required.")
class TestCommandWeb(bg_test_utils.TestCaseWithFakeAccessor):
    def tearDown(self):
        super(TestCommandWeb, self).tearDown()
        # Reset the registry to make sure we can instantiate the app again.
        # TODO: Try to use mock instead to do that.
        prometheus_client.REGISTRY = prometheus_client.core.CollectorRegistry(
            auto_describe=True
        )

    def test_run(self):
        cmd = command_web.CommandWeb()

        parser = argparse.ArgumentParser()
        bg_settings.add_argparse_arguments(parser)
        cmd.add_arguments(parser)
        opts = parser.parse_args(["--dry-run"])
        cmd.run(self.accessor, opts)


if __name__ == "__main__":
    unittest.main()
