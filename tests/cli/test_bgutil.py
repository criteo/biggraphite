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

from mock import patch
from six import StringIO

from biggraphite.cli import bgutil
from tests import test_utils as bg_test_utils


class TestBgutil(bg_test_utils.TestCaseWithFakeAccessor):

    metrics = ["metric1", "metric2"]

    @patch("sys.stdout", new_callable=StringIO)
    def test_run(self, mock_stdout):
        self.accessor.drop_all_metrics()
        for metric in self.metrics:
            self.accessor.create_metric(bg_test_utils.make_metric(metric))
        bgutil.main(["--driver=memory", "read", "**"], self.accessor)
        output = mock_stdout.getvalue()
        for metric in self.metrics:
            self.assertIn(metric, output)


if __name__ == "__main__":
    unittest.main()
