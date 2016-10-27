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
"""Clean Command."""


import logging

from biggraphite.cli import command
from biggraphite import metadata_cache


class CommandClean(command.BaseCommand):
    """Clean BigGraphite disk cache."""

    NAME = "clean"
    HELP = "Clean the disk cache."

    def run(self, accessor, opts):
        """Run some repairs.

        See command.BaseCommand
        """
        accessor.connect()

        if opts.storage_dir:
            cache = metadata_cache.DiskCache(accessor, opts.storage_dir)
            cache.open()
            cache.clean()
        else:
            logging.warning('Cannot clean disk cache because storage_dir is empty')
