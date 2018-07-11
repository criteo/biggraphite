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
from __future__ import absolute_import
from __future__ import print_function

import unittest

import mock

from biggraphite.drivers import _utils


class CountDownTest(unittest.TestCase):

    _COUNT = 42

    def setUp(self):
        self.on_zero = mock.Mock()
        self.count_down = _utils.CountDown(self._COUNT, self.on_zero)

    def test_on_failure(self):
        exc = Exception()
        self.count_down.on_failure(exc)
        self.on_zero.assert_called_once()

        # Failing again should not call the callback again.
        self.count_down.on_failure(exc)
        self.on_zero.assert_called_once()

    def test_on_result(self):
        result = "whatever this is not used"
        for _ in range(self._COUNT - 1):
            self.count_down.on_result(result)
            self.on_zero.assert_not_called()
        self.count_down.on_result(result)
        self.on_zero.assert_called_with(None)


if __name__ == "__main__":
    unittest.main()
