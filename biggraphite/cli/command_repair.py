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
import os
import sys

import progressbar

from biggraphite import metadata_cache
from biggraphite.cli import command

_DEV_NULL = open(os.devnull, "w")


class CommandRepair(command.BaseCommand):
    """Check that you can use BigGraphite."""

    NAME = "repair"
    HELP = "Run some repairs."

    def __init__(self):
        """Constructor."""
        self.pbar = None

    def add_arguments(self, parser):
        """Add custom arguments."""
        command.add_sharding_arguments(parser)
        parser.add_argument(
            "--quiet",
            action="store_const",
            default=False,
            const=True,
            help="Show no output unless there are problems.",
        )

    def run(self, accessor, opts, on_progress=None):
        """Run some repairs.

        See command.BaseCommand
        """
        accessor.connect()

        # TODO: use multiprocessing + progressbar here. Probably remove some
        # of the current arguments and generate them instead based on a number
        # of processes to do a full scan.

        if opts.storage_dir:
            settings = {"path": opts.storage_dir}
            with metadata_cache.DiskCache(accessor, settings) as cache:
                cache.repair(
                    shard=opts.shard,
                    nshards=opts.nshards,
                    start_key=opts.start_key,
                    end_key=opts.end_key,
                )
        else:
            logging.warning("Skipping disk cache repair because storage_dir is empty")

        out_fd = sys.stderr
        if opts.quiet:
            out_fd = _DEV_NULL

        if self.pbar is None:
            self.pbar = progressbar.ProgressBar(fd=out_fd, redirect_stderr=False)
        self.pbar.start()

        if on_progress is None:

            def _on_progress(done, total):
                self.pbar.max_value = max(total, done)
                self.pbar.update(done)

            on_progress = _on_progress

        accessor.repair(
            shard=opts.shard,
            nshards=opts.nshards,
            start_key=opts.start_key,
            end_key=opts.end_key,
            callback_on_progress=on_progress,
        )

        self.pbar.finish()
