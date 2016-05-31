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

from os import path as os_path
import tempfile
import shutil
import time
import unittest

import whisper

from biggraphite.cli import import_whisper
from biggraphite import test_utils as bg_test_utils


bg_test_utils.prepare_graphite_imports()


class TestMain(bg_test_utils.TestCaseWithFakeAccessor):

    def setUp(self):
        super(TestMain, self).setUp()
        self.patch_accessor()
        self.tempdir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tempdir)

    def test_single_metric(self):
        xfilesfactor = 0.5
        aggregation_method = "last"
        retentions = [(1, 60)]
        now = int(time.time())
        time_from, time_to = now - 10, now
        points = [(t, now-t) for t in xrange(time_from, time_to)]
        metric = "test_metric"
        metric_path = os_path.join(self.tempdir, metric + ".wsp")
        whisper.create(metric_path, retentions, xfilesfactor, aggregation_method)
        whisper.update_many(metric_path, points)

        self._call_main()

        meta = self.accessor.get_metric(metric)
        self.assertTrue(meta)
        self.assertEqual(meta.name, metric)
        self.assertEqual(meta.carbon_aggregation, aggregation_method)
        self.assertEqual(meta.carbon_xfilesfactor, xfilesfactor)
        self.assertEqual(meta.carbon_retentions, retentions)

        points_again = self.accessor.fetch_points(metric, time_from, time_to, step=1)
        self.assertEqual(points, points_again)

    def _call_main(self):
        import_whisper.main([
            "--quiet",
            "--keyspace", "keyspace",
            "--port", "42",
            "--process", "1",
            self.tempdir,
            "testhost1", "testhost2",
        ])


if __name__ == "__main__":
    unittest.main()
