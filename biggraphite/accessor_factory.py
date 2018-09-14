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
"""Module to create accessors."""

from biggraphite.drivers import cassandra as bg_cassandra
from biggraphite.drivers import elasticsearch as bg_elasticsearch
from biggraphite.drivers import hybrid as bg_hybrid
from biggraphite.drivers import memory as bg_memory


DRIVERS = frozenset(
    [
        ("cassandra", bg_cassandra),
        ("elasticsearch", bg_elasticsearch),
        ("memory", bg_memory),
    ]
)
DEFAULT_DRIVER = "cassandra"


class Error(Exception):
    """Base class for all exceptions from this module."""

    pass


class ConfigError(Error):
    """Configuration problems."""

    pass


def add_argparse_arguments(parser):
    """Add driver related BigGraphite arguments to an argparse parser.

    Args:
      parser: argparse.ArgumentParser()
    """
    parser.add_argument(
        "--driver",
        help="BigGraphite driver (%s)" % ", ".join([v[0] for v in DRIVERS]),
        default=DEFAULT_DRIVER,
    )
    parser.add_argument(
        "--metadata_driver",
        help="BigGraphite metadata driver (%s)" % ", ".join([v[0] for v in DRIVERS]),
    )
    parser.add_argument(
        "--data_driver",
        help="BigGraphite data driver (%s)" % ", ".join([v[0] for v in DRIVERS]),
    )


def accessor_from_settings(settings):
    """Get Accessor from configuration.

    Args:
      settings: dict(str -> value).

    Returns:
      Accessor (not connected).
    """
    driver_name = settings.get("driver", DEFAULT_DRIVER)
    metadata_driver = settings.get("metadata_driver", None)
    data_driver = settings.get("data_driver", None)

    if metadata_driver is None and data_driver is None:
        return _build_simple_accessor(driver_name, settings)
    else:
        if metadata_driver is None:
            raise ConfigError(
                "Metadata driver is not provided. Please specify --metadata_driver"
            )
        if data_driver is None:
            raise ConfigError(
                "Data driver is not provided. Please specify --data_driver"
            )

        # The data accessor may still update the metadata in its own storage
        # (update_on on insert_points(), read_on on fetch_points()). Since we
        # are in hybrid mode, the metadata accessor is responsible for these
        # actions, therefore we can disable the metadata handling in the data
        # accessor.
        enable_metadata_key = data_driver + '_enable_metadata'
        settings[enable_metadata_key] = False

        metadata_accessor = _build_simple_accessor(metadata_driver, settings)
        data_accessor = _build_simple_accessor(data_driver, settings)

        return bg_hybrid.HybridAccessor(
            "%s_%s" % (metadata_driver, data_driver), metadata_accessor, data_accessor
        )


def add_driver_options(options):
    """Add options from drivers."""
    for name, driver in DRIVERS:
        options.update({("%s_" % name) + k: v for k, v in driver.OPTIONS.items()})
    return options


def _build_simple_accessor(driver_name, settings):
    driver_settings = {}

    # Get driver specific settings.
    prefix = driver_name + "_"
    for key, value in settings.items():
        if key.startswith(prefix):
            key = key[len(prefix):]
            driver_settings[key] = value

    for name, driver in DRIVERS:
        if name == driver_name:
            return driver.build(**driver_settings)

    raise ConfigError("Invalid driver '%s'." % driver_name)
