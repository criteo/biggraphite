#!/usr/bin/env python
# Copyright 2016 Criteo
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
from mock import Mock, patch
from StringIO import StringIO

from biggraphite.cli import bgutil
from biggraphite import test_utils as bg_test_utils
from biggraphite.accessor import Metric, Retention


class TestBgutil(bg_test_utils.TestCaseWithFakeAccessor):

    metrics = ['metric1', 'metric2']

    @patch('biggraphite.drivers.memory._MemoryAccessor.get_metric')
    @patch('biggraphite.drivers.memory._MemoryAccessor.glob_metric_names')
    @patch('sys.stdout', new_callable=StringIO)
    def test_run(self, mock_stdout, mock_glob_metric_names, mock_get_metric):
        mock_glob_metric_names.return_value = self.metrics
        mock_get_metric.side_effect = lambda name: Mock(
            name=name,
            spec=Metric,
            metadata=Mock(  # mock.name not working as expected but ok for this test
                retention=Retention.from_string('1440*60s')))

        bgutil.main(['--driver=memory', 'read', '**'])


if __name__ == "__main__":
    unittest.main()
