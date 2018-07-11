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
"""Utilities factoring code across tests.

The dependencies for this module are not included in requirements.txt or in the package
dependencies, instead one needs the elements of tests-requirements.txt .
"""
from __future__ import absolute_import
from __future__ import print_function

import os
import sys
import tempfile
import shutil
import unittest
import logging
import uuid

import mock

from biggraphite import utils as bg_utils
from biggraphite import accessor as bg_accessor
from biggraphite.test_utils_cassandra import CassandraHelper
from biggraphite.drivers import memory as bg_memory
from biggraphite import metadata_cache as bg_metadata_cache


def setup_logging():
    """To be called to enable logs in unittests."""
    logger = logging.getLogger()
    stream_handler = logging.StreamHandler(sys.stdout)
    logger.addHandler(stream_handler)


def prepare_graphite():
    """Make sure that we have a working Graphite environment."""
    # Setup sys.path
    prepare_graphite_imports()
    os.environ['DJANGO_SETTINGS_MODULE'] = 'graphite.settings'

    # Redirect logs somewhere writable
    from django.conf import settings
    settings.LOG_DIR = tempfile.gettempdir()

    # Setup Django
    import django
    django.setup()


def prepare_graphite_imports():
    """Add to graphite libs to sys.path."""
    try:
        import carbon  # noqa
    except ImportError:
        to_add = ["/opt/graphite/lib", "/opt/graphite/webapp"]
        if os.environ.get("VIRTUAL_ENV"):
            # Running in a virtual environment
            for package_path in sys.path:
                if package_path.endswith("site-packages"):
                    for module_path in list(to_add):
                        venv_module_path = package_path + module_path
                        if os.path.isdir(venv_module_path):
                            # Replace the path in the list.
                            to_add.remove(module_path)
                            to_add.append(venv_module_path)

        # Add all custom paths to sys.path.
        for path in to_add:
            if path not in sys.path:
                sys.path.insert(0, path)


_UUID_NAMESPACE = uuid.UUID('{00000000-0000-0000-0000-000000000000}')


def make_metric(name, metadata=None, accessor=None, **kwargs):
    """Create a bg_accessor.Metric with specified metadata."""
    encoded_name = bg_accessor.encode_metric_name(name)
    retention = kwargs.get("retention")
    if isinstance(retention, str):
        kwargs["retention"] = bg_accessor.Retention.from_string(retention)
    if metadata:
        assert isinstance(metadata, bg_accessor.MetricMetadata)
        assert not kwargs
    else:
        metadata = bg_accessor.MetricMetadata(**kwargs)
    if not accessor:
        uid = uuid.uuid5(_UUID_NAMESPACE, str(encoded_name))
        return bg_accessor.Metric(name, uid, metadata)
    else:
        return accessor.make_metric(name, metadata)


def _make_easily_queryable_points(start, end, period):
    """Return points that aggregates easily.

    Averaging over each period gives range((end-start)/period).
    Taking last or max for each period gives [x*3 for x in range(end-start)].
    Taking min for each period gives [-1]*(end-start).
    """
    assert period % 4 == 0
    assert start % period == 0
    subperiod = int(period / 4)
    res = []
    for t in range(start, end, period):
        current_period = (t - start) // period
        # A fourth of points are -1
        res.append((t + 0 * subperiod, -1))
        # A fourth of points are +1
        res.append((t + 1 * subperiod, +1))
        # A fourth of points are the start timestamp
        res.append((t + 2 * subperiod, current_period * 3))
        # A fourth of points are missing

    return res


class TestCaseWithTempDir(unittest.TestCase):
    """A TestCase with a temporary directory."""

    def get_accessor(self):
        """Return the accessor."""
        return self.accessor

    def setUp(self):
        """Create a new temporary directory in self.tempdir."""
        super(TestCaseWithTempDir, self).setUp()
        self.tempdir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tempdir)


class TestCaseWithFakeAccessor(TestCaseWithTempDir):
    """A TestCase with a FakeAccessor."""

    KEYSPACE = "fake_keyspace"
    CACHE_CLASS = bg_metadata_cache.MemoryCache

    def setUp(self):
        """Create a new Accessor in self.acessor."""
        super(TestCaseWithFakeAccessor, self).setUp()
        self.accessor = bg_memory.build()
        self.accessor.connect()
        self.addCleanup(self.accessor.shutdown)
        self.metadata_cache = self.CACHE_CLASS(
            self.accessor, {'path': self.tempdir, 'size': 1024 * 1024})
        self.metadata_cache.open()
        self.addCleanup(self.metadata_cache.close)

    def fake_drivers(self):
        """Hijack drivers' build() functions to return self.accessor."""
        patcher = mock.patch('biggraphite.drivers.cassandra.build',
                             return_value=self.accessor)
        patcher.start()
        self.addCleanup(patcher.stop)

    def make_metric(self, name, metadata=None, **kwargs):
        """Create a bg_accessor.Metric with specified metadata."""
        return make_metric(name, metadata=metadata, accessor=self.accessor, **kwargs)


class TestCaseWithAccessor(TestCaseWithTempDir):
    """A TestCase with an Accessor."""

    ACCESSOR_SETTINGS = {}
    CACHE_CLASS = bg_metadata_cache.MemoryCache

    cassandra_helper = None

    @classmethod
    def setUpClass(cls):
        """Create the test Accessor."""
        driver_name = cls.ACCESSOR_SETTINGS.get('driver', bg_utils.DEFAULT_DRIVER)
        if "cassandra" in driver_name:
            cls.cassandra_helper = CassandraHelper()
            cls.cassandra_helper.setUpClass()
            cls.ACCESSOR_SETTINGS.update(
                cls.cassandra_helper.get_accessor_settings())

        cls.accessor = bg_utils.accessor_from_settings(cls.ACCESSOR_SETTINGS)
        cls.accessor.syncdb()
        cls.accessor.connect()

    @classmethod
    def tearDownClass(cls):
        """Stop the test Accessor."""
        super(TestCaseWithAccessor, cls).tearDownClass()
        cls.accessor.shutdown()
        if cls.cassandra_helper:
            cls.cassandra_helper.tearDownClass()

    def setUp(self):
        """Create a new Accessor in self.acessor."""
        super(TestCaseWithAccessor, self).setUp()
        self.metadata_cache = self.CACHE_CLASS(
            self.accessor, {'path': self.tempdir, 'size': 1024 * 1024})
        self.metadata_cache.open()

    def tearDown(self):
        """Cleanup after tests."""
        super(TestCaseWithAccessor, self).tearDown()
        self.metadata_cache.close()
        self.accessor.flush()
        self.accessor.clear()
        self.__drop_all_metrics()

    def flush(self):
        """Flush all kind of buffers."""
        self.accessor.flush()
        if self.cassandra_helper:
            self.cassandra_helper.flush(self.accessor)

    def make_metric(self, name, metadata=None, **kwargs):
        """Create a bg_accessor.Metric with specified metadata."""
        return make_metric(name, metadata=metadata, accessor=self.accessor, **kwargs)

    def __drop_all_metrics(self):
        self.accessor.connect()
        self.accessor.drop_all_metrics()
