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
"""Graphite utility module."""

from os import path as os_path

from biggraphite import utils as bg_utils


class Error(Exception):
    """Base class for all exceptions from this module."""

    pass


class ConfigError(Error):
    """Configuration problems."""

    pass


def accessor_from_settings(settings):
    """Get Accessor from Graphite-related configuration object.

    Args:
      settings: either carbon_conf.Settings or a Django-like settings object.

    Returns:
      Accessor (not connected).
    """
    settings = bg_utils.settings_from_confattr(settings)
    bg_utils.set_log_level(settings)
    return bg_utils.accessor_from_settings(settings)


def storage_path(settings):
    """Get storage path from configuration.

    Args:
      settings: either carbon_conf.Settings or a Django-like settings object

    Returns:
      An absolute path.
    """
    path, found = bg_utils.get_setting(settings, "STORAGE_DIR")
    if not path or not os_path.exists(path):
        raise ConfigError("STORAGE_DIR is set to an unexisting directory: '%s'" % path)
    return os_path.abspath(path)
