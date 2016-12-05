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

    def add_arguments(self, parser):
        """Add custom arguments."""
        parser.add_argument(
            "--start-key",
            help="Start key.",
        )
        parser.add_argument(
            "--end-key",
            help="End key.",
        )
        parser.add_argument(
            "--shard",
            help="Shard number.",
            type=int,
            default=0,
        )
        parser.add_argument(
            "--nshards",
            help="Number of shards.",
            type=int,
            default=1,
        )

    def run(self, accessor, opts):
        """Run some repairs.

        See command.BaseCommand
        """
        accessor.connect()

        # TODO: use multiprocessing + progressbar here. Probably remove some
        # of the current arguments and generate them instead based on a number
        # of processes to do a full scan.

        if opts.storage_dir:
            with metadata_cache.DiskCache(accessor, opts.storage_dir) as cache:
                cache.repair(shard=opts.shard, nshards=opts.nshards,
                             start_key=opts.start_key,
                             end_key=opts.end_key)
        else:
            logging.warning('Skipping disk cache repair because storage_dir is empty')

        def log_progress(progress, range_beg, range_end):
            logging.info("Repair done at %d %% for [%d, %d] keys"
                         % (progress, range_beg, range_end))

        accessor.repair(shard=opts.shard, nshards=opts.nshards,
                        start_key=opts.start_key,
                        end_key=opts.end_key,
                        callback_on_progress=log_progress)
