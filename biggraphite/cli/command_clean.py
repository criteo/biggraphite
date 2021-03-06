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
import os
import sys
import time

import progressbar

from biggraphite import metadata_cache
from biggraphite.cli import command

from prometheus_client import write_to_textfile, REGISTRY

_DEV_NULL = open(os.devnull, "w")


class CommandClean(command.BaseCommand):
    """Clean BigGraphite metric metadata from old metrics."""

    NAME = "clean"
    HELP = "Clean the metric metadata."

    def __init__(self):
        """Constructor."""
        self.pbar = None
        self.metrics_file_path = ""

    def add_arguments(self, parser):
        """Add custom arguments."""
        parser.add_argument("--clean-cache", help="clean cache", action="store_true")
        parser.add_argument(
            "--clean-backend", help="clean backend", action="store_true"
        )
        parser.add_argument(
            "--clean-corrupted", help="clean corrupted metrics", action="store_true"
        )
        parser.add_argument(
            "--quiet",
            action="store_const",
            default=False,
            const=True,
            help="Show no output unless there are problems.",
        )
        parser.add_argument(
            "--max-age",
            help="Specify the age of metrics in seconds to evict"
            " (ie: 3600 to delete older than one hour metrics)",
            type=int,
            default=3 * 24 * 60 * 60,
            action="store",
        )
        parser.add_argument(
            "--metrics-file-path",
            help="Dump metrics in file",
            type=str,
            default="",
            action="store"
        )
        command.add_sharding_arguments(parser)
        command.add_clean_arguments(parser)

    def run(self, accessor, opts, on_progress=None):
        """Run some cleanups.

        See command.BaseCommand
        """
        out_fd = sys.stderr
        if opts.quiet:
            out_fd = _DEV_NULL

        self.metrics_file_path = opts.metrics_file_path

        if self.pbar is None:
            start_key = -1 * 2**63
            end_key = 2**63 - 1

            if opts.start_key is not None:
                start_key = int(opts.start_key)
            if opts.end_key is not None:
                end_key = int(opts.end_key)

            widgets = [
                progressbar.Variable('token', format='(current: {formatted_value})'),
                ' ',
                progressbar.Percentage(),
                ' ',
                progressbar.SimpleProgress(
                    format='(%s)' % progressbar.SimpleProgress.DEFAULT_FORMAT
                ),
                ' ',
                progressbar.Bar(),
                ' ',
                progressbar.Timer(),
                ' ',
                progressbar.AdaptiveETA(),
            ]

            # max_value = end_key - start_key
            self.pbar = progressbar.ProgressBar(
                widgets=widgets,
                fd=out_fd,
                redirect_stderr=False,
                min_value=0,
                max_value=end_key - start_key)

        self.pbar.start()

        if on_progress is None:
            def _on_progress(total, done, token):
                self.pbar.update(total, token=token)

                if self.metrics_file_path != "":
                    write_to_textfile(self.metrics_file_path, REGISTRY)

            on_progress = _on_progress

        accessor.connect()

        if opts.clean_cache:
            if opts.storage_dir:
                settings = {"path": opts.storage_dir, "ttl": opts.max_age}

                logging.info("Cleaning cache from %s", settings)
                with metadata_cache.DiskCache(accessor, settings) as cache:
                    cache.clean()
            else:
                logging.error("Cannot clean disk cache because storage_dir" " is empty")

        if opts.clean_backend:
            logging.info("Cleaning backend, removing things before %d", opts.max_age)
            accessor.clean(
                max_age=opts.max_age,
                shard=opts.shard,
                nshards=opts.nshards,
                start_key=opts.start_key,
                end_key=opts.end_key,
                callback_on_progress=on_progress,
                disable_clean_directories=opts.disable_clean_directories,
                disable_clean_metrics=opts.disable_clean_metrics,
            )

        if opts.clean_corrupted:
            # Remove corrupt metrics.
            now = time.time()

            def callback(metric, done, total):
                # TODO: Probably worth removing old metrics here
                # instead of in the driver... The index doesn't work
                # well anyway.
                if metric.updated_on:
                    delta = now - time.mktime(metric.updated_on.timetuple())
                else:
                    delta = now
                if delta > opts.max_age:
                    logging.info("Removing %s (%s)" % (metric.name, delta))
                    accessor.delete_metric(metric.name)
                on_progress(done, total)

            def errback(metric):
                logging.info("Removing %s" % metric)
                accessor.delete_metric(metric)

            logging.info("Cleaning corrupted metrics")
            accessor.map(
                callback,
                shard=opts.shard,
                nshards=opts.nshards,
                start_key=opts.start_key,
                end_key=opts.end_key,
                errback=errback,
            )

        self.pbar.finish()

        # Final metric dump
        if self.metrics_file_path != "":
            write_to_textfile(self.metrics_file_path, REGISTRY)
