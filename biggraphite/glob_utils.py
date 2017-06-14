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

from enum import Enum
import itertools
import re

# http://graphite.readthedocs.io/en/latest/render_api.html#paths-and-wildcards
_GRAPHITE_GLOB_RE = re.compile(r"^[^*?{}\[\]]+$")


def _is_graphite_glob(metric_component):
    """Return whether a metric component is a Graphite glob."""
    return _GRAPHITE_GLOB_RE.match(metric_component) is None


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


class TokenType(Enum):
    """Represents atomic types used to tokenize Graphite globbing patterns."""

    PATH_SEPARATOR = 0
    LITERAL = 1
    WILD_CHAR = 2
    WILD_SEQUENCE = 3
    WILD_PATH = 4
    CHAR_SELECT_BEGIN = 5
    CHAR_SELECT_NEGATED_BEGIN = 6
    CHAR_SELECT_RANGE_DASH = 7
    CHAR_SELECT_END = 8
    EXPR_SELECT_BEGIN = 9
    EXPR_SELECT_SEPARATOR = 10
    EXPR_SELECT_END = 11


def tokenize(glob):
    """Convert a glob expression to a stream of tokens.

    Tokens have the form (type: TokenType, data: String).

    Args:
      glob: Graphite glob pattern.

    Returns:
      Iterator on a token stream.
    """
    SPECIAL_CHARS = '.?*[-]{,}'
    is_escaped = False
    is_char_select = False
    tmp = ""
    token = None
    i = -1
    while i+1 < len(glob):
        i += 1
        c = glob[i]

        # Literal handling
        if is_escaped:
            tmp += c
            is_escaped = False
            continue
        elif c == '\\':
            is_escaped = True
            continue
        elif c not in SPECIAL_CHARS or (c == '-' and not is_char_select):
            if token and token != TokenType.LITERAL:
                yield token, None
                token, tmp = TokenType.LITERAL, ""
            token = TokenType.LITERAL
            tmp += c
            continue
        elif token:
            yield token, tmp
            token, tmp = None, ""

        # Special chars handling
        if c == '.':
            yield TokenType.PATH_SEPARATOR, ""
        elif c == '?':
            yield TokenType.WILD_CHAR, ""
        elif c == '*':
            # Look-ahead for wild path (globstar)
            if i+1 < len(glob) and glob[i+1] == '*':
                i += 1
                yield TokenType.WILD_PATH, ""
            else:
                yield TokenType.WILD_SEQUENCE, ""
        elif c == '[':
            is_char_select = True
            # Look-ahead for negated selector (not in)
            if i+1 < len(glob) and glob[i+1] == '!':
                i += 1
                yield TokenType.CHAR_SELECT_NEGATED_BEGIN, ""
            else:
                yield TokenType.CHAR_SELECT_BEGIN, ""
        elif c == '-':
            yield TokenType.CHAR_SELECT_RANGE_DASH, ""
        elif c == ']':
            is_char_select = False
            yield TokenType.CHAR_SELECT_END, ""
        elif c == '{':
            yield TokenType.EXPR_SELECT_BEGIN, ""
        elif c == ',':
            yield TokenType.EXPR_SELECT_SEPARATOR, ""
        elif c == '}':
            yield TokenType.EXPR_SELECT_END, ""
        else:
            raise Exception("Unexpected character '%s'" % c)

    # Do not forget trailing token, if any
    if token:
        yield token, tmp


def _glob_to_regex(glob):
    """Convert a Graphite globbing pattern into a regular expression.

    This function does not check for glob validity, if you want usable regexes
    then you must check _is_valid_glob() first.

    Uses _tokenize() to obtain a token stream, then does simple substitution
    from token type and data to equivalent regular expression.

    It handles * as being anything except a dot.
    It returns a regex that only matches whole strings (i.e. ^regex$).

    Args:
      glob: Valid Graphite glob pattern.

    Returns:
      Regex corresponding to the provided glob.
    """
    ans = ""
    for token, data in tokenize(glob):
        if token == TokenType.PATH_SEPARATOR:
            ans += re.escape('.')
        elif token == TokenType.LITERAL:
            ans += re.escape(data)
        elif token == TokenType.WILD_CHAR:
            ans += "."
        elif token == TokenType.WILD_SEQUENCE:
            ans += "[^.]*"
        elif token == TokenType.WILD_PATH:
            ans += ".*"
        elif token == TokenType.CHAR_SELECT_BEGIN:
            ans += "["
        elif token == TokenType.CHAR_SELECT_NEGATED_BEGIN:
            ans += "[^"
        elif token == TokenType.CHAR_SELECT_RANGE_DASH:
            ans += "-"
        elif token == TokenType.CHAR_SELECT_END:
            ans += "]"
        elif token == TokenType.EXPR_SELECT_BEGIN:
            ans += "("
        elif token == TokenType.EXPR_SELECT_SEPARATOR:
            ans += "|"
        elif token == TokenType.EXPR_SELECT_END:
            ans += ")"
        else:
            raise Exception("Unexpected token type '%s' with data '%s'" % (token, data))
    return '^' + ans + '$'


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


def graphite_glob(accessor, graphite_glob, metrics=True, directories=True):
    """Get metrics and directories matching a Graphite glob.

    Args:
      accessor: BigGraphite accessor
      graphite_glob: Graphite glob expression
      metrics: True if metrics should be fetched
      directories: True if directories should be fetched

    Returns:
      A tuple:
        First element: sorted list of Cassandra metrics matched by the glob.
        Second element: sorted list of Cassandra directories matched by the glob.
    """
    if not _is_valid_glob(graphite_glob):
        # TODO(d.forest): should we instead raise an exception?
        return ([], [])

    glob_re = re.compile(_glob_to_regex(graphite_glob))

    if metrics:
        metrics = accessor.glob_metric_names(graphite_glob)
        metrics = filter(glob_re.match, metrics)
    else:
        metrics = []

    if directories:
        directories = accessor.glob_directory_names(graphite_glob)
        directories = filter(glob_re.match, directories)
    else:
        directories = []

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
    """Represents any single character."""

    pass


class AnySequence(GlobExpression):
    """Represents any sequence of 0 or more characters."""

    pass


class SequenceIn(GlobExpressionWithValues):
    """Represents a choice between different character sequences."""

    pass


class GraphiteGlobParser:
    """Utility class for parsing graphite glob expressions."""

    # TODO(d.forest): upgrade to new tokenizer here
    # TODO(d.forest): after upgrade, try to improve Cassandra query generation

    def __init__(self):
        """Build a parser, fill in default values."""
        self._reset('')

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

    def _find_char_selector_end(self, i, n):
        """Find where a character selector expression ends."""
        j = i
        if j < n and self._glob[j] == '!':
            j += 1
        if j < n and self._glob[j] == ']':
            j += 1

        j = self._glob.find(']', j)
        if j == -1:
            return n

        return j

    def _parse_char_selector(self, i, n):
        """Parse character selector, with support for negation and ranges.

        For simplicity (and because it does not seem useful at the moment) we
        will not be generating the possible values or parsing the ranges, but
        outputting AnyChar on valid char selector expressions.
        """
        j = self._find_char_selector_end(i, n)
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
        """Parse character sequence selector, with support for nesting.

        For simplicity, we will be outputting AnySequence in situations where
        values contain a character selector.
        """
        result = self._parse_sequence_selector_values(i, n)
        if result:
            has_char_selector, i, values = result
            if not has_char_selector and len(values) == 1:
                self._sequence += values[0]
            else:
                if has_char_selector:
                    seq = AnySequence()
                else:
                    seq = SequenceIn(values)

                self._commit_sequence()
                self._component.append(seq)
        else:
            self._sequence += '{'

        return i

    def _parse_sequence_selector_values(self, i, n):
        has_char_selector = False
        values = []
        curr = []
        tmp = ''
        j = i
        c = ''
        while j < n and c != '}':
            c = self._glob[j]
            j += 1
            # Parse sub-expression then combine values with prefixes.
            if c == '{':
                if tmp != '':
                    curr.append(tmp)
                    tmp = ''

                result = self._parse_sequence_selector_values(j, n)
                if not result:
                    return None

                has_charsel, j, subvalues = result
                has_char_selector = has_char_selector or has_charsel
                curr = [prefix + x for prefix in curr for x in subvalues]
            # End of current element, combine values with suffix.
            elif c == ',' or c == '}':
                if len(curr) > 0:
                    values += [x + tmp for x in curr]
                else:
                    values.append(tmp)

                curr = []
                tmp = ''
            # Simplified handling of char selector
            elif c == '[':
                # XXX(d.forest): We could keep track of depth and just make sure
                #                the selector expression is well-formed instead
                #                of continuing to parse everything.
                #                This is open for later improvement.
                k = self._find_char_selector_end(j, n)
                if k < n:
                    has_char_selector = True
                    j = k + 1
                else:
                    tmp += '['
            # Reject dots inside selectors
            elif c == '.':
                return None
            # Append char to the current value.
            else:
                tmp += c

        # We have reached the end without finding a closing brace: the braces
        # are unbalanced, expression cannot be parsed as a sequence selector.
        if j == n and c != '}':
            return None

        return has_char_selector, j, values + curr

    def _reset(self, glob):
        self._glob = glob
        self._parsed = []
        self._component = []
        self._sequence = ''

    def parse(self, glob):
        """Parse a graphite glob expression into simple components."""
        self._reset(glob)

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
