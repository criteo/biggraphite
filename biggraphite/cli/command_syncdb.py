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
"""Write command."""

from __future__ import print_function

from biggraphite.cli import command


class CommandSyncdb(command.BaseCommand):
    """Syncdb points."""

    NAME = "syncdb"
    HELP = "create or upgrade the database schema."

    def add_arguments(self, parser):
        """Add custom arguments."""
        parser.add_argument(
            "--dry_run", action="store_const", default=False, const=True,
            help="Only show commands to create/upgrade the schema.")

    def run(self, accessor, opts):
        """Run the command."""
        schema = accessor.syncdb(dry_run=opts.dry_run)
        if opts.dry_run:
            print(schema)
        else:
            accessor.connect()
