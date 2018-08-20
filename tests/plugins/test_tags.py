#!/usr/bin/env python
# Copyright 2017 Criteo
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
# See the License for the specific lanbg_guage governing permissions and
# limitations under the License.

from __future__ import absolute_import
from __future__ import print_function

import unittest

from tests import test_utils as bg_test_utils

# This needs to run before we import the plugin.
bg_test_utils.prepare_graphite()

try:
    from biggraphite.plugins import tags as bg_tags  # noqa

    HAVE_TAGS = True
except ImportError:
    HAVE_TAGS = False


@unittest.skipUnless(HAVE_TAGS, "This version of Graphite doesn't support tags")
class TestTags(bg_test_utils.TestCaseWithFakeAccessor):
    def setUp(self):
        super(TestTags, self).setUp()
        self.accessor.connect()

        from django.conf import settings as django_settings

        self.tagdb = bg_tags.BigGraphiteTagDB(
            settings=django_settings,
            accessor=self.accessor,
            metadata_cache=self.metadata_cache,
        )

    def testBasic(self):
        # FIXME: add more tests when things are implemented
        pass


if __name__ == "__main__":
    unittest.main()
