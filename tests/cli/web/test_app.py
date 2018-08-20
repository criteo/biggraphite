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
"""Test the WebApp."""

import unittest

import prometheus_client

try:
    import gourde
    from biggraphite.cli.web import app
except ImportError:
    gourde = None

from tests import test_utils as bg_test_utils


@unittest.skipUnless(gourde, "Gourde is required.")
class TestWebApp(bg_test_utils.TestCaseWithFakeAccessor):
    def setUp(self):
        super(TestWebApp, self).setUp()
        # Don't use a shared registry.
        self.registry = prometheus_client.CollectorRegistry(auto_describe=True)
        self.webapp = app.WebApp(registry=self.registry)

        parser = gourde.Gourde.get_argparser()
        args = parser.parse_args([])
        self.webapp.initialize_app(self.accessor, args)
        self.client = self.webapp.app.test_client()

    def test_index(self):
        rv = self.client.get("/")
        self.assertEqual(rv.status_code, 200)

    def test_bgutil_list(self):
        rv = self.client.post("/api/bgutil/list", json={"arguments": ["*"]})
        rv.get_json()


if __name__ == "__main__":
    unittest.main()
