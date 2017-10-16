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
"""Tag support for Graphite and Carbon."""

from __future__ import absolute_import  # Otherwise graphite is this module.


from graphite.tags import utils


class BigGraphiteTagDB(utils.BaseTagDB):
    """TagDB using BigGraphite."""

    def __init__(self, accessor=None, metadata_cache=None):
        """Creates a BigGraphiteTagDB."""
        super(BigGraphiteTagDB, self).__init__()

        self._accessor = accessor
        self._cache = metadata_cache

    def find_series(self, tags):
        """Find series by tag.

        Accepts a list of tag specifiers and returns a list of matching paths.

        Tags specifiers are strings, and may have the following formats:

        .. code-block:: none

        tag=spec    tag value exactly matches spec
        tag!=spec   tag value does not exactly match spec
        tag=~value  tag value matches the regular expression spec
        tag!=~spec  tag value does not match the regular expression spec

        Any tag spec that matches an empty value is considered to match series
        that don't have that tag.

        At least one tag spec must require a non-empty value.

        Regular expression conditions are treated as being anchored at the start
        of the value.

        Matching paths are returned as a list of strings.
        """
        return []

    def get_series(self, path):
        """Get series by path.

        Accepts a path string and returns a TaggedSeries object describing the
        series.

        If the path is not found in the TagDB, returns None.
        """
        return []

    def list_tags(self, tagFilter=None):
        """List defined tags.

        Returns a list of dictionaries describing the tags stored in the TagDB.

        Each tag dict contains the key "tag" which holds the name of the tag.
        Additional keys may be returned.

        .. code-block:: none

        [
          {
            'tag': 'tag1',
          },
        ]

        Accepts an optional filter parameter which is a regular expression used
        to filter the list of returned tags
        """
        return []

    def get_tag(self, tag, valueFilter=None):
        """Get details of a particular tag.

        Accepts a tag name and returns a dict describing the tag.

        The dict contains the key "tag" which holds the name of the tag.
        It also includes a "values" key, which holds a list of the values
        for each tag.  See list_values() for the structure of each value.

        .. code-block:: none

        {
          'tag': 'tag1',
          'values': [
            {
              'value': 'value1',
              'count': 1,
            }
          ],
        }

        Accepts an optional filter parameter which is a regular expression
        used to filter the list of returned tags.
        """
        return []

    def list_values(self, tag, valueFilter=None):
        """List values for a particular tag.

        Returns a list of dictionaries describing the values stored in the TagDB.

        Each value dict contains the key "value" which holds the value,
        and the key "count" which is the number of series that have that value.
        Additional keys may be returned.

        .. code-block:: none

        [
          {
            'value': 'value1',
            'count': 1,
          },
        ]

        Accepts an optional filter parameter which is a regular expression used
        to filter the list of returned tags.
        """
        return []

    def tag_series(self, series):
        """Enter series into database.

        Accepts a series string, upserts into the TagDB and returns the
        canonicalized series name.
        """
        pass

    def del_series(self, series):
        """Remove series from database.

        Accepts a series string and returns True.
        """
        return []
