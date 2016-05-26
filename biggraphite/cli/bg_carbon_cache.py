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
"""Simulates carbon-cache after loading the carbon plugin."""
from __future__ import print_function

import sys

from carbon import util as carbon_util
from carbon import exceptions as carbon_exceptions

try:
    from biggraphite.plugins import carbon  # noqa
except ImportError as e:
    print("Failed importing the biggraphite plugin:\n%s" % e, file=sys.stderr)


def main():
    """The entry point of this module."""
    try:
        carbon_util.run_twistd_plugin(__file__)
    except carbon_exceptions.CarbonConfigException as exc:
        # This is what carbon cache does, we preserve that behaviour.
        raise SystemExit(str(exc))


if __name__ == "__main__":
    main()
