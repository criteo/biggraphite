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
"""Shell Command."""

from biggraphite.cli import command


class CommandShell(command.BaseCommand):
    """Open a shell to use BigGraphite."""

    NAME = "shell"
    HELP = "Open a shell."

    def run(self, accessor, opts):
        """Open a shell.

        See command.BaseCommand
        """
        header = (
            "`accessor` contains BigGraphite accesor of type %s\n"
            " run `accessor.connect()` to use it"
        ) % (accessor.backend_name)

        from IPython import embed

        embed(header=header)
