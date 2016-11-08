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

import time
import logging

from biggraphite.cli import command
from biggraphite import metadata_cache


class CommandClean(command.BaseCommand):
    """Clean BigGraphite metric metadata from old metrics."""

    NAME = "clean"
    HELP = "Clean the metric metadata."

    def add_arguments(self, parser):
        """Add custom arguments."""
        parser.add_argument(
            "--cache",
            help="clean cache",
            action='store_true'
        )
        parser.add_argument(
            "--backend",
            help="clean backend",
            action='store_true'
        )
        parser.add_argument(
            "--cutoff",
            help="specify cutoff time in UNIX time",
            type=int
        )
        parser.add_argument(
            "--batch-size",
            help="specify batch size for backend processing",
            type=int,
            default=10000,
        )

    def run(self, accessor, opts):
        """Run some cleanup.

        See command.BaseCommand
        """
        accessor.connect()

        if not opts.cutoff:
            cutoff = int(time.time()) - 24*60*60
        else:
            cutoff = opts.cutoff
        logging.info("Setting cutoff time to: %d", cutoff)

        if opts.cache:
            if opts.storage_dir:
                cache = metadata_cache.DiskCache(accessor, opts.storage_dir)
                cache.open()
                cache.clean()
            else:
                logging.warning('Cannot clean disk cache because storage_dir is empty')

        if opts.backend:
            accessor.clean(cutoff, opts.batch_size)
