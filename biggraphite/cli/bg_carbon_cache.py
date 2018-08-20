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
"""Simulates carbon-cache after loading the carbon plugin.

This fail is required as there is no way to load a plugin from
carbon configuration, such as what
https://github.com/graphite-project/carbon/pull/139
implements.
"""
from __future__ import absolute_import
from __future__ import print_function

import sys

from biggraphite import utils as bg_utils


def main(_executable=sys.argv[0], _sys_path=sys.path):
    """The entry point of this module."""
    bg_utils.manipulate_paths_like_upstream(_executable, _sys_path)
    from carbon import util as carbon_util
    from carbon import exceptions as carbon_exceptions

    # Importing the plugin registers it.
    from biggraphite.plugins import carbon as unused_carbon  # noqa

    try:
        # The carbon code tries to guess GRAPHITE_ROOT from the filename
        # given to run_twistd_plugin() to set GRAPHITE_ROOT. This is then
        # used to setup default paths. Try to make it somewhat compatible
        # when carbon is installed in its default directory.
        bg_utils.setup_graphite_root_path(carbon_util.__file__)
        carbon_util.run_twistd_plugin("carbon-cache")
    except carbon_exceptions.CarbonConfigException as exc:
        # This is what carbon cache does, we preserve that behaviour.
        raise SystemExit(str(exc))


if __name__ == "__main__":
    main()
