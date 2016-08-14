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

from os import path as os_path
import os

from biggraphite.drivers import cassandra as bg_cassandra
from biggraphite.drivers import memory as bg_memory


DEFAULT_DRIVER = "cassandra"
DEFAULT_CASSANDRA_KEYSPACE = "biggraphite"
DEFAULT_CASSANDRA_CONTACT_POINTS = "127.0.0.1"
DEFAULT_CASSANDRA_PORT = 9042
DEFAULT_CASSANDRA_TIMEOUT = 10.0
DEFAULT_CASSANDRA_CONNECTIONS = 4


class Error(Exception):
    """Base class for all exceptions from this module."""

    pass


class ConfigError(Error):
    """Configuration problems."""

    pass


# TODO(c.chary): we probably want our own config file at some point, this
#  file could be used by command line utilities. Alternatively we can
#  make them load settings from existing config files.

def _get_setting(settings, name, optional=False, default=None):
    # Only way we found to support both Carbon & Django settings.
    res = default
    try:
        res = settings[name]
    except (TypeError, KeyError):
        try:
            res = getattr(settings, name)
        except:
            pass
    # Fallback to env.
    if res is None and name in os.environ:
        res = os.environ[name]
    if res is None and not optional:
        raise ConfigError("%s missing in configuration" % name)
    return res


def list_from_str(value):
    """Convert a comma separated string into a list.

    Args:
      value: str or list or set.

    Returns:
      list a list of values.
    """
    if type(value) is str:
        value = [s.strip() for s in value.split(",")]
    elif type(value) in (list, set):
        value = list(value)
    else:
        raise ConfigError("Unkown type for '%s'" % (value))
    return value


def cassandra_accessor_from_settings(settings):
    """Get Cassandra Accessor from configuration.

    Args:
      settings: either carbon_conf.Settings or a Django-like settings object

    Returns:
      Cassandra accessor (not connected)
    """
    options = (
        ("keyspace", "BG_CASSANDRA_KEYSPACE", str, True),
        ("contact_points", "BG_CASSANDRA_CONTACT_POINTS", list_from_str, False),
        ("port", "BG_CASSANDRA_PORT", int, True),
        ("concurrency", "BG_CASSANDRA_CONNECTIONS", int, True),
        ("default_timeout", "BG_CASSANDRA_TIMEOUT", int, True),
    )
    kwargs = {}
    for name, up_name, func, optional in options:
        value = _get_setting(settings, up_name, optional=optional)
        if value is None:
            continue
        kwargs[name] = func(value)
    return bg_cassandra.build(**kwargs)


def memory_accessor_from_settings(settings):
    """Get Memory Accessor from configuration.

    Args:
      settings: either carbon_conf.Settings or a Django-like settings object

    Returns:
      Memory accessor (not connected)
    """
    return bg_memory.build()


def accessor_from_settings(settings):
    """Get Accessor from configuration.

    Args:
      settings: either carbon_conf.Settings or a Django-like settings object

    Returns:
      Accessor (not connected)
    """
    drivers = (
        ("cassandra", cassandra_accessor_from_settings),
        ("memory", memory_accessor_from_settings),
    )
    driver_name = _get_setting(settings, "BG_DRIVER", default=DEFAULT_DRIVER)
    for name, builder in drivers:
        if name == driver_name:
            return builder(settings)
    raise ConfigError("Invalid value '%s' for option 'BG_DRIVER'" % driver_name)


def add_argparse_arguments(parser):
    """Add generic BigGraphite arguments to an argparse parser.

    Args:
      parser: argparse.ArgumentParser()
    """
    parser.add_argument("--driver",
                        help="BigGraphite driver ('cassandra' or 'memory')",
                        default=DEFAULT_DRIVER)
    parser.add_argument("--cassandra_keyspace", metavar="NAME",
                        help="Cassandra keyspace.",
                        default=DEFAULT_CASSANDRA_KEYSPACE)
    parser.add_argument("--cassandra_contact_points", metavar="HOST", nargs="+",
                        help="Hosts used for discovery.",
                        default=DEFAULT_CASSANDRA_CONTACT_POINTS)
    parser.add_argument("--cassandra_port", metavar="PORT", type=int,
                        help="The native port to connect to.",
                        default=DEFAULT_CASSANDRA_PORT)
    parser.add_argument("--cassandra_connections", metavar="N", type=int,
                        help="Number of connections per Cassandra host per process.",
                        default=DEFAULT_CASSANDRA_CONNECTIONS)
    parser.add_argument("--cassandra_timeout", metavar="TIMEOUT", type=int,
                        help="Cassandra query timeout in seconds.",
                        default=DEFAULT_CASSANDRA_TIMEOUT)


def settings_from_args(args):
    """Create settings from args.

    Args:
      args: argparse.Namespace, parsed arguments.

    Returns:
      dict(string: value): settings
    """
    return {
        'BG_DRIVER': args.driver,
        'BG_CASSANDRA_KEYSPACE': args.cassandra_keyspace,
        'BG_CASSANDRA_CONTACT_POINTS': args.cassandra_contact_points,
        'BG_CASSANDRA_PORT': args.cassandra_port,
        'BG_CASSANDRA_CONNECTIONS': args.cassandra_connections,
        'BG_CASSANDRA_TIMEOUT': args.cassandra_timeout,
    }


def storage_path_from_settings(settings):
    """Get storage path from configuration.

    Args:
      settings: either carbon_conf.Settings or a Django-like settings object

    Returns:
      An absolute path.
    """
    path = _get_setting(settings, "STORAGE_DIR")
    if not os_path.exists(path):
        raise ConfigError("STORAGE_DIR is set to an unexisting directory: '%s'" % path)
    return os_path.abspath(path)
