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

import prometheus_client

DEFAULT_LOG_LEVEL = "WARNING"
DEFAULT_ADMIN_PORT = None


log = logging.getLogger(__name__)


class Error(Exception):
    """Base class for all exceptions from this module."""

    pass


class ConfigError(Error):
    """Configuration problems."""

    pass


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
            os.environ["GRAPHITE_ROOT"] = "/opt/graphite"


def round_down(rounded, divider):
    """Round down an integer to a multiple of divider."""
    return int(rounded) // divider * divider


def round_up(rounded, divider):
    """Round up an integer to a multiple of divider."""
    return int(rounded + divider - 1) // divider * divider
