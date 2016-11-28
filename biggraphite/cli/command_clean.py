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
import time

from biggraphite.cli import command
from biggraphite import metadata_cache


class CommandClean(command.BaseCommand):
    """Clean BigGraphite metric metadata from old metrics."""

    NAME = "clean"
    HELP = "Clean the metric metadata."

    def add_arguments(self, parser):
        """Add custom arguments."""
        parser.add_argument(
            "--clean-cache",
            help="clean cache",
            action='store_true'
        )
        parser.add_argument(
            "--clean-backend",
            help="clean backend",
            action='store_true'
        )
        parser.add_argument(
            "--cutoff",
            help="specify cutoff time in UNIX time",
            type=int,
            default=int(time.time()) - 24 * 60 * 60,
            action='store'
        )

    def run(self, accessor, opts):
        """Run some cleanups.

        See command.BaseCommand
        """
        with accessor as bg_acc:
            if opts.clean_cache:
                if opts.storage_dir:
                    settings = {"path": opts.storage_dir, "ttl": opts.cutoff}

                    logging.info("Cleaning cache from %s", settings)
                    with metadata_cache.DiskCache(bg_acc, settings) as cache:
                        cache.clean()
                else:
                    logging.error('Cannot clean disk cache because storage_dir is empty')

            if opts.clean_backend:
                logging.info("Cleaning backend, removing things before %d", opts.cutoff)
                bg_acc.clean(opts.cutoff)
