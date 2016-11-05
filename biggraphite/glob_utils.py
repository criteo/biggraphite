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
"""Globbing utility module."""

import itertools
import re

# http://graphite.readthedocs.io/en/latest/render_api.html#paths-and-wildcards
_GRAPHITE_GLOB_RE = re.compile(r"^[^*?{}\[\]]+$")


def _is_graphite_glob(metric_component):
    """Return whether a metric component is a Graphite glob."""
    return _GRAPHITE_GLOB_RE.match(metric_component) is None


def _graphite_glob_to_accessor_components(graphite_glob):
    """Transform Graphite glob into Cassandra accessor components."""
    return ".".join([
        "**" if c == "**" else
        "*" if _is_graphite_glob(c) else
        c
        for c in graphite_glob.split(".")
    ])


def _is_valid_glob(glob):
    """Check whether a glob pattern is valid.

    It does so by making sure it has no dots (path separator) inside groups,
    and that the grouping braces are not mismatched. This helps doing useless
    (or worse, wrong) work on queries.

    Args:
      glob: Graphite glob pattern.

    Returns:
      True if the glob is valid.
    """
    depth = 0
    for c in glob:
        if c == '{':
            depth += 1
        elif c == '}':
            depth -= 1
            if depth < 0:
                # Mismatched braces
                return False
        elif c == '.':
            if depth > 0:
                # Component separator in the middle of a group
                return False
    # We should have exited all groups at the end
    return depth == 0


def _glob_to_regex(glob):
    """Convert a Graphite globbing globtern into a regular expression.

    This is a modified implementation of fnmatch.translate that behaves slightly
    differently. It now handles the brace syntax for value choice (e.g. {aa,ab}
    for aa or ab). It handles * as being anything except a dot. It also returns
    a regex that only matches whole strings, and that does not handle multiline
    matches.

    Args:
      glob: Valid Graphite glob pattern.

    Returns:
      Regex corresponding to the provided glob.
    """
    i = 0
    n = len(glob)
    res = ''
    while i < n:
        c = glob[i]
        i += 1
        # Single character wildcard
        if c == '?':
            res += '.'
        # Multi-character wildcard and globstar
        elif c == '*':
            # Look-ahead for potential globstar
            if i < n and glob[i] == '*':
                res += '.*'
                i += 1
            else:
                res += '[^.]*'
        # Character selection (with negation)
        elif c == '[':
            j = i
            if j < n and glob[j] == '!':
                j += 1
            if j < n and glob[j] == ']':
                j += 1
            while j < n and glob[j] != ']':
                j += 1
            if j >= n:
                res = res + '\\['
            else:
                tmp = glob[i:j].replace('\\', '\\\\')
                i = j+1
                if tmp[0] == '!':
                    tmp = '^' + tmp[1:]
                elif tmp[0] == '^':
                    tmp = '\\' + tmp
                res += '[' + tmp + ']'
        # Word selection (supports nesting)
        elif c == '{':
            depth = 1
            tmp = '('
            j = i
            # Look-ahead for the whole brace-enclosed expression
            while j < n and depth > 0:
                c = glob[j]
                j += 1
                if c == '{':
                    tmp += '('
                    depth += 1
                elif c == '}':
                    tmp += ')'
                    depth -= 1
                elif c == ',':
                    tmp += '|'
                else:
                    tmp += c
            # Escape the current brace in case it is mismatched
            if depth > 0:
                res += '\\{'
            else:
                i = j
                res += tmp
        # Default: escape character
        else:
            res = res + re.escape(c)
    return '^' + res + '$'


def glob(metric_names, glob_pattern):
    """Pre-filter metric names according to a glob expression.

    Uses the dot-count and the litteral components of the glob to filter
    guaranteed non-matching values out, but may still require post-filtering.

    Args:
      metric_names: List of metric names to be filtered.
      glob_pattern: Glob pattern.

    Returns:
      List of metric names that may be matched by the provided glob.
    """
    glob_components = glob_pattern.split(".")

    globstar = None
    prefix_literals = []
    suffix_literals = []
    for (index, component) in enumerate(glob_components):
        if component == "**":
            globstar = index
        elif globstar:
            # Indexed relative to the end because globstar length is arbitrary
            suffix_literals.append((len(glob_components) - index, component))
        elif not _is_graphite_glob(component):
            prefix_literals.append((index, component))

    def maybe_matched_prefilter(metric):
        metric_components = metric.split(".")
        if globstar:
            if len(metric_components) < len(glob_components):
                return False
        elif len(metric_components) != len(glob_components):
            return False

        for (index, value) in itertools.chain(suffix_literals, prefix_literals):
            if metric_components[index] != value:
                return False

        return True

    return filter(maybe_matched_prefilter, metric_names)


def graphite_glob(accessor, graphite_glob):
    """Get metrics and directories matching a Graphite glob.

    Args:
      accessor: BigGraphite accessor
      graphite_glob: Graphite glob expression

    Returns:
      A tuple:
        First element: sorted list of Cassandra metrics matched by the glob.
        Second element: sorted list of Cassandra directories matched by the glob.
    """
    if not _is_valid_glob(graphite_glob):
        # TODO(d.forest): should we instead raise an exception?
        return ([], [])

    glob_re = re.compile(_glob_to_regex(graphite_glob))
    accessor_components = _graphite_glob_to_accessor_components(graphite_glob)

    metrics = accessor.glob_metric_names(accessor_components)
    metrics = filter(glob_re.match, metrics)

    directories = accessor.glob_directory_names(accessor_components)
    directories = filter(glob_re.match, directories)
    return (metrics, directories)


class GlobExpression:
    """Base class for glob expressions."""

    def __repr__(self):
        return self.__class__.__name__

    def __eq__(self, other):
        return self.__class__ == other.__class__


class GlobExpressionWithValues(GlobExpression):
    """Base class for glob expressions that have values."""

    def __init__(self, values):
        """Take a list of values, and stores the sorted unique values."""
        self.values = sorted(set(values))

    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, self.values)

    def __eq__(self, other):
        return (GlobExpression.__eq__(self, other) and
                self.values == other.values)


class Globstar(GlobExpression):
    """Represents a globstar wildcard."""

    pass


class AnyChar(GlobExpression):
    """Represents any single character in a glob."""

    pass


class AnySequence(GlobExpression):
    """Represents any sequence of 0 or more characters in a glob."""

    pass


class GraphiteGlobParser:
    """Utility class for parsing graphite glob expressions."""

    def __init__(self):
        """Build a parser, fill in default values."""
        self._glob = ''
        self._reset()

    def _commit_sequence(self):
        if len(self._sequence) > 0:
            self._component.append(self._sequence)
            self._sequence = ''

    def _commit_component(self):
        self._commit_sequence()
        if len(self._component) > 0:
            self._parsed.append(self._component)
            self._component = []

    def _parse_char_wildcard(self):
        """Parse single character wildcard."""
        self._commit_sequence()
        self._component.append(AnyChar())

    def _parse_wildcard(self, i, n):
        """Parse multi-character wildcard, and globstar."""
        self._commit_sequence()
        # Look-ahead for potential globstar
        if i < n and self._glob[i] == '*':
            self._commit_component()
            self._parsed.append(Globstar())
            i += 1
        else:
            self._component.append(AnySequence())

        return i

    def _parse_char_selector(self, i, n):
        """Parse character selector, with support for negation and ranges.

        For simplicity (and because it does not seem useful at the moment) we
        will not be generating the possible values or parsing the ranges, but
        outputting AnyChar on valid char selector expressions.
        """
        j = i
        if j < n and self._glob[j] == '!':
            j += 1
        if j < n and self._glob[j] == ']':
            j += 1
        while j < n and self._glob[j] != ']':
            j += 1

        if j < n:
            # +1 to skip closing bracket
            i = j + 1
            self._commit_sequence()
            self._component.append(AnyChar())
        else:
            # Reached end of string: unbalanced bracket
            self._sequence += '['

        return i

    def _parse_sequence_selector(self, i, n):
        """Parse character sequence selector, with nesting.

        For simplicity (and because it does not seem useful at the moment) we
        will not be generating the possible values, but outputting AnySequence
        on valid char sequence selector expressions.
        """
        depth = 1
        j = i
        while j < n and depth > 0:
            c = self._glob[j]
            j += 1
            if c == '{':
                depth += 1
            elif c == '}':
                depth -= 1
            elif c == '.':
                # We have a component separator, this selector is wrong
                #
                # XXX(d.forest): this is purposefully simplified, but maybe we
                #                should handle escaped dots, or dots within char
                #                selectors?
                break

        if depth == 0:
            i = j
            self._commit_sequence()
            self._component.append(AnySequence())
        else:
            # Reached end of string: braces are unbalanced
            self._sequence += '{'

        return i

    def _reset(self, glob=''):
        if glob == self._glob:
            return

        self._glob = glob
        self._parsed = []
        self._component = []
        self._sequence = ''

    def parse(self, glob):
        """Parse a graphite glob expression into simple components."""
        self._reset(glob)
        if self._parsed:
            return self._parsed

        i = 0
        n = len(self._glob)
        while i < n:
            c = self._glob[i]
            i += 1
            if c == '?':
                self._parse_char_wildcard()
            elif c == '*':
                i = self._parse_wildcard(i, n)
            elif c == '[':
                i = self._parse_char_selector(i, n)
            elif c == '{':
                i = self._parse_sequence_selector(i, n)
            elif c == '.':
                self._commit_component()
            else:
                self._sequence += c

        self._commit_component()
        return self._parsed
