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

from six.moves.configparser import ConfigParser

try:
    from carbon import util as carbon_util

    HAVE_CARBON = True
except ImportError:
    HAVE_CARBON = False

from biggraphite.cli import command
from biggraphite import metric as bg_metric


class CommandSyncdb(command.BaseCommand):
    """Syncdb points."""

    NAME = "syncdb"
    HELP = "create or upgrade the database schema."

    def add_arguments(self, parser):
        """Add custom arguments."""
        parser.add_argument(
            "--dry_run",
            action="store_const",
            default=False,
            const=True,
            help="Only show commands to create/upgrade the schema.",
        )
        if HAVE_CARBON:
            parser.add_argument(
                "--storage-schemas",
                help="Create tables from this Carbon's storage-schemas.conf file.",
                required=False,
            )
        parser.add_argument(
            "--retention", help="Retention to create.", default=None, required=False
        )

    def _get_retentions_from_storage_schemas(self, opts):
        """Parse storage-schemas.conf and returns all retentions."""
        ret = []

        config_parser = ConfigParser()
        if not config_parser.read(opts.storage_schemas):
            raise SystemExit(
                "Error: Couldn't read config file: %s" % opts.storage_schemas
            )
        for section in config_parser.sections():
            options = dict(config_parser.items(section))
            retentions = options["retentions"].split(",")
            archives = [carbon_util.parseRetentionDef(s) for s in retentions]
            ret.append(bg_metric.Retention.from_carbon(archives))

        return ret

    def run(self, accessor, opts):
        """Run the command."""
        retentions = []

        if HAVE_CARBON and opts.storage_schemas:
            retentions.extend(self._get_retentions_from_storage_schemas(opts))
        if opts.retention:
            retentions.extend([bg_metric.Retention.from_string(opts.retention)])

        schema = accessor.syncdb(retentions=retentions, dry_run=opts.dry_run)
        if opts.dry_run:
            if schema:
                print(schema)
        else:
            accessor.connect()
