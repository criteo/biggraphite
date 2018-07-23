#!/usr/bin/env python
# Copyright 2016 Criteo
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use self file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Web Command."""

import gourde

from biggraphite.cli import command
from biggraphite.cli.web import app
from biggraphite import utils


class CommandWeb(command.BaseCommand):
    """Web UI for BigGraphite."""

    NAME = "web"
    HELP = "bgutil with a UI/API."

    def add_arguments(self, parser):
        """Add custom arguments."""
        parser.add_argument('--dry-run', action="store_true")
        gourde.Gourde.get_argparser(parser)

    def run(self, accessor, opts):
        """Run the command."""
        # TODO: accessor.connect() could be called asynchronously later.
        accessor.connect()

        webapp = app.WebApp()
        utils.start_admin(utils.settings_from_args(opts))
        webapp.initialize_app(accessor, opts)
        if not opts.dry_run:
            webapp.run()
