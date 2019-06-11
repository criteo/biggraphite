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
"""Utility module."""

import logging
import os
import threading

import prometheus_client

DEFAULT_LOG_LEVEL = "WARNING"
DEFAULT_ADMIN_PORT = None
DEFAULT_GRAPHITE_ROOT = '/opt/graphite'


log = logging.getLogger(__name__)


class Error(Exception):
    """Base class for all exceptions from this module."""

    pass


class ConfigError(Error):
    """Configuration problems."""

    pass


class FileWatcher:
    """Watcher that checks if a file exists."""

    def __init__(self, filename):
        """The constructor takes the filename to be watched."""
        self._filename = filename

    def watch(self):
        """Returns True if the file exists."""
        return os.path.exists(self._filename)


class FeatureSwitch:
    """FeatureSwitch checks if a feature is enabled or not."""

    def __init__(self, watcher, default_value):
        """Constructor for FeatureSwitch.

        Args:
          watcher: the watcher object used to determine if the feature is enabled or not
          default_value: bool, value when the watcher object returns False
        """
        self._watcher = watcher
        self._default_value = default_value

        # flag holding watcher's state
        # Using an Event because this class is meant to be shared by several threads
        self._flag = threading.Event()

        # initializate the internal state
        self.watch()

    def enabled(self):
        """Check if the feature is enabled or not."""
        if self._default_value:
            return not self._flag.is_set()
        else:
            return self._flag.is_set()

    def watch(self):
        """Refresh the internal state with the value returned by the watcher object."""
        if self._watcher.watch():
            self._flag.set()
        else:
            self._flag.clear()


def start_admin(settings):
    """Start the admin interface.

    Args:
      settings: dict(str -> value).
    """
    port = settings.get("admin_port", DEFAULT_ADMIN_PORT)
    if port and not start_admin.started:
        prometheus_client.start_wsgi_server(port)
        start_admin.started = True


start_admin.started = False


def set_log_level(settings):
    """Set logs level according to settings."""
    logger = logging.getLogger("biggraphite")
    # Install a handler if there are none.
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
        )
        logger.addHandler(handler)
    logger.setLevel(settings.get("loglevel", DEFAULT_LOG_LEVEL))


def manipulate_paths_like_upstream(_executable, sys_path):
    """Replicate the sys.path magic from carbon-aggregator-cache.

    Upstream's carbon-aggregator-cache adds the lib sister directory of its
    parent bin directory to sys.path. This does the same.
    """
    bin_dir = os.path.dirname(os.path.abspath(_executable))
    root_dir = os.path.dirname(bin_dir)
    lib_dir = os.path.join(root_dir, "lib")
    sys_path.insert(0, lib_dir)


def setup_graphite_root_path(carbon_util_file):
    """Setup GRAPHITE_ROOT.

    This is then used to setup default paths. Try to make it somewhat compatible
    when carbon is installed in its default directory.
    """
    if os.path.dirname(carbon_util_file) == "/opt/graphite/lib/carbon":
        if "GRAPHITE_ROOT" not in os.environ:
            os.environ["GRAPHITE_ROOT"] = DEFAULT_GRAPHITE_ROOT


def round_down(rounded, divider):
    """Round down an integer to a multiple of divider."""
    return int(rounded) // divider * divider


def round_up(rounded, divider):
    """Round up an integer to a multiple of divider."""
    return int(rounded + divider - 1) // divider * divider
