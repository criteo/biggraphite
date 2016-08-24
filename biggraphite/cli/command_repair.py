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
"""Repair Command."""


import logging

from biggraphite.cli import command
from biggraphite import metadata_cache


class CommandRepair(command.BaseCommand):
    """Check that you can use BigGraphite."""

    NAME = "repair"
    HELP = "Run some repairs."

    def run(self, accessor, opts):
        """Run some repairs.

        See command.BaseCommand
        """
        accessor.connect()

        if opts.storage_path:
            cache = metadata_cache.DiskCache(accessor, opts.storage_path)
            # TODO: Once repair() has support for start_key and end_key it will be
            # easier to parallelise this command.
            cache.open()
            cache.repair()
        else:
            logging.warning(
                'Skipping disk cache repair because storage_path is empty')

        accessor.repair()
