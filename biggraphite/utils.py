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

from biggraphite.drivers import cassandra as bg_cassandra
from biggraphite.drivers import memory as bg_memory


DRIVERS = frozenset([
    ("cassandra", bg_cassandra),
    ("memory", bg_memory),
])

DEFAULT_DRIVER = "cassandra"
DEFAULT_LOG_LEVEL = "WARNING"
OPTIONS = {
    "driver": str,
    "loglevel": str,
}


class Error(Exception):
    """Base class for all exceptions from this module."""

    pass


class ConfigError(Error):
    """Configuration problems."""

    pass


def accessor_from_settings(settings):
    """Get Accessor from configuration.

    Arguments:
      settings: dict(str -> value).

    Returns:
      Accessor (not connected).
    """
    driver_name = settings.get('driver', DEFAULT_DRIVER)
    driver_settings = {}

    # Get driver specific settings.
    prefix = driver_name + '_'
    for key, value in settings.items():
        if key.startswith(prefix):
            key = key[len(prefix):]
            driver_settings[key] = value

    for name, driver in DRIVERS:
        if name == driver_name:
            return driver.build(**driver_settings)

    raise ConfigError("Invalid value '%s' for BG_DRIVER." % driver_name)


def add_argparse_arguments(parser):
    """Add generic BigGraphite arguments to an argparse parser.

    Args:
      parser: argparse.ArgumentParser()
    """
    parser.add_argument(
        "--driver",
        help="BigGraphite driver ('cassandra' or 'memory')",
        default=DEFAULT_DRIVER)
    parser.add_argument(
        "--storage_path", metavar="PATH",
        help="Storage path (cache, etc..)")
    parser.add_argument(
        "--loglevel", metavar="LEVEL",
        help="Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)",
        default=DEFAULT_LOG_LEVEL)
    bg_cassandra.add_argparse_arguments(parser)


def set_log_level(settings):
    """Set logs level according to settings."""
    logger = logging.getLogger()
    logger.setLevel(settings.get("loglevel", DEFAULT_LOG_LEVEL))


def get_setting(settings, name):
    """Get a specific setting from Carbon/Django like settings."""
    res = None
    found = False
    try:
        res = settings[name]
        found = True
    except (TypeError, KeyError):
        try:
            res = getattr(settings, name)
            found = True
        except:
            pass
    return res, found


def settings_from_args(args):
    """Create settings dict from args.

    Args:
      args: argparse.Namespace, parsed arguments.

    Returns:
      dict(string: value): settings
    """
    return settings_from_confattr(args, prefix="")


def settings_from_confattr(conf, prefix="bg_"):
    """Create settings dict from Django/Carbon like settings.

    Args:
      settings: either carbon_conf.Settings or a Django-like settings object

    Returns:
      dict(string: value): settings
    """
    settings = {}

    options = dict(OPTIONS)
    for name, driver in DRIVERS:
        options.update(
            {('%s_' % name) + k: v for k, v in driver.OPTIONS.items()})

    for option, validator in options.items():
        option_u = (prefix + option).upper()
        option_l = (prefix + option).lower()
        value, found = get_setting(conf, option_u)
        if not found:
            value, found = get_setting(conf, option_l)
        if found:
            settings[option] = validator(value)

    return settings
