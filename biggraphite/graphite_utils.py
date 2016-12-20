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

from biggraphite import utils as bg_utils


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


def cache_from_settings(accessor, settings):
    """Get Cache from Graphite-related configuration object.

    Args:
      accessor: a connected Accessor.
      settings: either carbon_conf.Settings or a Django-like settings object.

    Returns:
      Cache (not opened).
    """
    settings = bg_utils.settings_from_confattr(settings)
    return bg_utils.cache_from_settings(accessor, settings)
