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
"""Delete Command."""

from __future__ import print_function

import logging

from biggraphite.cli import command


log = logging.getLogger(__name__)


class CommandDelete(command.BaseCommand):
    """Delete for metrics."""

    NAME = "delete"
    HELP = "Delete metrics."

    def add_arguments(self, parser):
        """Add custom arguments.

        See command.CommandBase.
        """
        parser.add_argument(
            "path",
            help="One metric or subdirectory name"
        )
        parser.add_argument(
            "-r",
            "--recursive",
            action="store_const", default=False, const=True,
            help="Delete points for all metrics as a subtree",
        )
        parser.add_argument(
            "--dry-run",
            action="store_const", default=False, const=True,
            help="Only show commands to create/upgrade the schema.",
            required=False,
        )

    def run(self, accessor, opts):
        """Delete metrics and directories.

        See command.CommandBase.
        """
        accessor.connect()

        message = ' (dry-run)' if opts.dry_run else ''
        pattern = "%s.**" % opts.path if opts.recursive else opts.path

        for metric in accessor.glob_metric_names(pattern):
            log.info("Removing metric%s: %s" % (message, metric))
            if not opts.dry_run:
                accessor.delete_metric(metric)

        for directory in accessor.glob_directory_names(pattern):
            log.info("Removing directory%s: %s" % (message, directory))
            if not opts.dry_run:
                accessor.delete_directory(directory)
