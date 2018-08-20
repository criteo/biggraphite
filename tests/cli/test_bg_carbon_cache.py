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

from tests import test_utils as bg_test_utils  # noqa

bg_test_utils.prepare_graphite_imports()  # noqa

import unittest

from carbon import exceptions as carbon_exceptions
from carbon import util as carbon_util
import mock

from biggraphite.cli import bg_carbon_cache


class TestMain(unittest.TestCase):
    def test_runs(self):
        sys_path = []
        with mock.patch.object(
            carbon_util, "run_twistd_plugin", return_value=None
        ) as run_twisted:
            bg_carbon_cache.main("/a/b/c/bin/bg-carbon-cache", sys_path)
        run_twisted.assert_called_once()
        self.assertEqual(1, len(sys_path))
        self.assertEqual("/a/b/c/lib", sys_path[0])

    def test_carbon_fails(self):
        with mock.patch.object(
            carbon_util,
            "run_twistd_plugin",
            side_effect=carbon_exceptions.CarbonConfigException,
        ) as run_twisted:
            self.assertRaises(SystemExit, bg_carbon_cache.main)
        run_twisted.assert_called_once()


if __name__ == "__main__":
    unittest.main()
