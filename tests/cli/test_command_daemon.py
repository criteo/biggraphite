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

import multiprocessing
import time
import unittest
import argparse

from biggraphite import utils as bg_utils
from biggraphite import test_utils as bg_test_utils
from biggraphite.cli import command_daemon


class TestCommandDaemon(bg_test_utils.TestCaseWithFakeAccessor):

    def test_run_daemon(self):
        self.accessor.drop_all_metrics()

        cmd = command_daemon.CommandDaemon()
        command_daemon._run_webserver = lambda x, y, z: time.sleep(666)

        parser = argparse.ArgumentParser()
        bg_utils.add_argparse_arguments(parser)
        cmd.add_arguments(parser)
        opts = parser.parse_args(
            ['--clean-backend', '--clean-cache', '--max-age=12']
        )

        def run():
            cmd.run(self.accessor, opts)

        p = multiprocessing.Process(target=run)

        p.start()
        time.sleep(1)
        assert(p.is_alive())
        p.terminate()


if __name__ == "__main__":
    unittest.main()
