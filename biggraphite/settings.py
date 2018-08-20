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
"""Module to handle settings."""
import distutils

from biggraphite import accessor_factory as bg_accessor_factory
from biggraphite import cache_factory as bg_cache_factory
from biggraphite.drivers import cassandra as bg_cassandra
from biggraphite.drivers import elasticsearch as bg_elasticsearch

DEFAULT_LOG_LEVEL = "WARNING"
DEFAULT_ADMIN_PORT = None


def strtoint(value):
    """Cast a string to an integer."""
    if value is None:
        return None
    return int(value)


def strtobool(value):
    """Cast a string to a bool."""
    if value is None:
        return None
    if type(value) is bool:
        return value
    return distutils.util.strtobool(value)


def strvalidator(value):
    """Cast a value to string."""
    if value is None:
        return None
    return str(value)


OPTIONS = {
    "driver": str,
    "metadata_driver": strvalidator,
    "data_driver": strvalidator,
    "cache": str,
    "cache_size": strtoint,
    "cache_ttl": strtoint,
    "cache_sync": strtobool,
    "loglevel": str,
    "storage_dir": str,
    "admin_port": strtoint,
}


def add_argparse_arguments(parser):
    """Add generic BigGraphite arguments to an argparse parser.

    Args:
      parser: argparse.ArgumentParser()
    """
    parser.add_argument(
        "--storage_dir", metavar="PATH", help="Storage path (cache, etc..)"
    )
    parser.add_argument(
        "--loglevel",
        metavar="LEVEL",
        help="Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)",
        default=DEFAULT_LOG_LEVEL,
    )
    parser.add_argument(
        "--admin_port",
        metavar="PORT",
        help="Admin port with /metrics",
        default=DEFAULT_ADMIN_PORT,
    )
    bg_accessor_factory.add_argparse_arguments(parser)
    bg_cache_factory.add_argparse_arguments(parser)
    bg_cassandra.add_argparse_arguments(parser)
    bg_elasticsearch.add_argparse_arguments(parser)


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
        except Exception:
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
    bg_accessor_factory.add_driver_options(options)

    for option, validator in options.items():
        option_u = (prefix + option).upper()
        option_l = (prefix + option).lower()
        value, found = get_setting(conf, option_u)
        if not found:
            value, found = get_setting(conf, option_l)
        if found:
            settings[option] = validator(value)

    return settings
