# Copyright 2018 Criteo
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

"""Cassandra indexing code using SASI."""

from __future__ import absolute_import
from __future__ import print_function

import itertools

from cassandra import encoder as c_encoder

from biggraphite import glob_utils as bg_glob
from biggraphite import accessor as bg_accessor
from biggraphite.drivers import cassandra_common


GLOBSTAR = bg_glob.Globstar()
ANYSEQUENCE = bg_glob.AnySequence()

_COMPONENTS_MAX_LEN = cassandra_common.COMPONENTS_MAX_LEN
_LAST_COMPONENT = cassandra_common.LAST_COMPONENT
DIRECTORY_SEPARATOR = cassandra_common.DIRECTORY_SEPARATOR


METADATA_CREATION_CQL_PARENT_INDEXES = [
    "CREATE CUSTOM INDEX IF NOT EXISTS ON \"%%(keyspace)s\".%(table)s (parent)"
    "  USING 'org.apache.cassandra.index.sasi.SASIIndex'"
    "  WITH OPTIONS = {"
    "    'analyzer_class': 'org.apache.cassandra.index.sasi.analyzer.NonTokenizingAnalyzer',"
    "    'case_sensitive': 'true'"
    "  };" % {"table": t}
    for t in ('metrics', 'directories')
]

METADATA_CREATION_CQL_PATH_INDEXES = [
    "CREATE CUSTOM INDEX IF NOT EXISTS ON \"%%(keyspace)s\".%(table)s (component_%(component)d)"
    "  USING 'org.apache.cassandra.index.sasi.SASIIndex'"
    "  WITH OPTIONS = {"
    "    'analyzer_class': 'org.apache.cassandra.index.sasi.analyzer.NonTokenizingAnalyzer',"
    "    'case_sensitive': 'true'"
    "  };" % {"table": t, "component": n}
    for t in ('metrics', 'directories')
    for n in range(_COMPONENTS_MAX_LEN)
]


class CassandraSASI(object):
    """Cassandra SASI Query generator."""

    def __init__(self, keyspace, max_queries_per_pattern, max_metrics_per_pattern):
        """Initialize the query generator."""
        self.keyspace_metadata = keyspace
        self.max_metrics_per_pattern = max_metrics_per_pattern
        self.max_queries_per_pattern = max_queries_per_pattern

    def generate_queries(self, table, components):
        """Generate queries based on parsed components."""
        if GLOBSTAR in components:
            queries = self.__generate_globstar_names_queries(table, components)
        else:
            components.append([_LAST_COMPONENT])
            queries = self.__generate_normal_names_queries(table, components)
        return queries

    def __generate_normal_names_queries(self, table, components):
        # Only keep the component parts that enable us to build prefix queries.
        # This means any uninterrupted sequence of strings or braces selectors.
        # On the way, we keep the position and value counts of selectors for
        # further query simplification.
        idxlens = []
        combinations = 1
        for cidx, component in enumerate(components):
            entry = []
            end = 0
            for pidx, part in enumerate(component):
                if isinstance(part, bg_glob.SequenceIn):
                    count = len(part.values)
                    combinations *= count
                    entry.append((pidx, count))
                elif not bg_glob.is_fixed_sequence(part):
                    # If we have globs we can't do much more.
                    break

                end = pidx + 1

            idxlens.append(entry)
            simplified_component = component[:end]
            if len(simplified_component) < len(component):
                simplified_component.append(ANYSEQUENCE)

            components[cidx] = simplified_component

        # Skip any additional work if we have a basic query.
        if combinations == 1:
            return [self.__build_select_names_query(table, components)]

        # Reduce complexity by dropping the rightmost selector from each
        # component, starting with the shallowest component, until we have a low
        # enough combination count.
        for cidx, entry in enumerate(idxlens):
            if combinations <= self.max_queries_per_pattern:
                break

            while len(entry) > 0 and combinations > self.max_queries_per_pattern:
                component = components[cidx]
                idx, count = entry.pop()

                surrounding_anyseqs = 0
                if idx > 0 and component[idx - 1] == ANYSEQUENCE:
                    surrounding_anyseqs += 1
                if idx < len(component) - 1 and component[idx + 1] == ANYSEQUENCE:
                    surrounding_anyseqs += 1

                # If we have surrounding AnySeqs, then drop elements so that
                # only one remains. Otherwise, replace current part with AnySeq.
                if surrounding_anyseqs > 0:
                    while surrounding_anyseqs > 0:
                        del (component[idx])
                        surrounding_anyseqs -= 1
                else:
                    component[idx] = ANYSEQUENCE

                combinations /= count

        # Pre-compute all possible values for each component.
        for cidx, component in enumerate(components):
            suffix = []
            if component[-1] == ANYSEQUENCE:
                if len(component) == 1:
                    components[cidx] = [component]
                    continue
                else:
                    suffix.append(ANYSEQUENCE)

            values = ['']
            for part in component:
                if bg_glob.is_fixed_sequence(part):
                    values = [x + part for x in values]
                elif isinstance(part, bg_glob.SequenceIn):
                    values = [x + y
                              for x in values
                              for y in part.values]
                else:
                    break

            components[cidx] = [[x] + suffix for x in values]

        # Generate queries using the combinations of pre-computed values for the
        # components.
        return [
            self.__build_select_names_query(table, combination)
            for combination in itertools.product(*components)
        ]

    def __generate_globstar_names_queries(self, table, components):
        # Handling more than one of these can cause combinatorial explosion.
        if components.count(GLOBSTAR) > 1:
            raise bg_accessor.InvalidGlobError(
                "Contains more than one globstar (**) operator"
            )

        # If the globstar operator is at the end of the pattern, then we can
        # find corresponding metrics with a prefix search;
        # otherwise, we have to generate incremental queries that go up to a
        # certain depth (_COMPONENTS_MAX_LEN - #components).
        gs_index = components.index(GLOBSTAR)
        if gs_index == len(components) - 1:
            return [
                self.__build_select_names_query(table, components[:gs_index])
            ]

        prefix = components[:gs_index]
        suffix = components[gs_index + 1:] + [[_LAST_COMPONENT]]
        max_wildcards = min(self.max_queries_per_pattern,
                            _COMPONENTS_MAX_LEN - len(components))
        return [
            self.__build_select_names_query(
                table,
                prefix + wildcards * [[bg_glob.AnySequence()]] + suffix,
            )
            for wildcards in range(1, max_wildcards)
        ]

    def __build_select_names_query(self, table, components):
        query_select = "SELECT name FROM \"%s\".\"%s\"" % (
            self.keyspace_metadata,
            table,
        )
        query_limit = "LIMIT %d" % (self.max_metrics_per_pattern + 1)

        if len(components) == 0:
            return "%s %s;" % (query_select, query_limit)

        # If all components are constant values we can search by exact name.
        # If all but the last component are constant values we can search by
        # exact parent, in which case we may benefit from filtering the last
        # component by prefix when we have one. (Code refers to the previous-to
        # -last component because of the __END__ suffix we use).
        #
        # We are not using prefix search on the parent because it appears to be
        # too slow/costly at the moment (see #174 for details).
        if (
                components[-1] == [_LAST_COMPONENT] and  # Not a prefix globstar
                all(len(c) == 1 and bg_glob.is_fixed_sequence(c[0])
                    for c in components[:-2])
        ):
            last = components[-2]
            if len(last) == 1 and bg_glob.is_fixed_sequence(last[0]):
                # XXX(d.forest): do not try to optimize by passing the raw glob
                #                and using it here; because this is invalid in
                #                cases where the glob contains braces.
                name = DIRECTORY_SEPARATOR.join(
                    itertools.chain.from_iterable(components[:-1]))
                return "%s WHERE name = %s %s;" % (
                    query_select,
                    c_encoder.cql_quote(name),
                    query_limit,
                )
            else:
                if len(last) > 0 and bg_glob.is_fixed_sequence(last[0]):
                    prefix_filter = "AND component_%d LIKE %s" % (
                        len(components) - 2,
                        c_encoder.cql_quote(last[0] + '%'),
                    )
                    allow_filtering = "ALLOW FILTERING"
                else:
                    prefix_filter = ''
                    allow_filtering = ''

                parent = itertools.chain.from_iterable(components[:-2])
                parent = DIRECTORY_SEPARATOR.join(parent) + DIRECTORY_SEPARATOR
                return "%s WHERE parent = %s %s %s %s;" % (
                    query_select,
                    c_encoder.cql_quote(parent),
                    prefix_filter,
                    query_limit,
                    allow_filtering,
                )

        where_clauses = []

        for n, component in enumerate(components):
            if len(component) == 0:
                continue

            # We are currently using prefix indexes, so if we do not have a
            # prefix value (i.e. it is a wildcard), then the current component
            # cannot be constrained inside the request.
            value = component[0]
            if not bg_glob.is_fixed_sequence(value):
                continue

            if len(component) == 1:
                op = '='
            else:
                op = "LIKE"
                value += '%'

            clause = "component_%d %s %s" % (n, op, c_encoder.cql_quote(value))
            where_clauses.append(clause)

        if len(where_clauses) == 0:
            return "%s %s;" % (query_select, query_limit)

        return "%s WHERE %s %s ALLOW FILTERING;" % (
            query_select,
            " AND ".join(where_clauses),
            query_limit
        )
