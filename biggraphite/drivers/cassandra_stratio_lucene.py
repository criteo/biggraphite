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

"""Cassandra indexing code using the Stratio Lucene plugin.

See https://github.com/Stratio/cassandra-lucene-index
"""
from __future__ import absolute_import
from __future__ import print_function

from cassandra import query as c_query

from biggraphite.drivers import lucene
from biggraphite.drivers import cassandra_common

_COMPONENTS_INDEX_MAX_LEN = cassandra_common.COMPONENTS_MAX_LEN

LUCENE_INDEX_REFRESH_PERIOD_SECOND = 61  # Default value FIXME: use it

_CQL_PATH_COMPONENTS_INDEX_FIELDS = ",\n".join(
    "        component_%d : {type: \"string\"}" % n for n in range(_COMPONENTS_INDEX_MAX_LEN)
)

CQL_CREATE_INDICES = [
    "CREATE CUSTOM INDEX IF NOT EXISTS %(table)s_idx ON \"%%(keyspace)s\".%(table)s()"
    "  USING 'com.stratio.cassandra.lucene.Index' WITH OPTIONS = {"
    "    'refresh_seconds': '60',"
    "    'schema': '{"
    "      fields: {"
    "        parent : {type: \"string\"},"
    "%(components)s"
    "      }"
    "    }'"
    "  };" % {"table": t, "components": _CQL_PATH_COMPONENTS_INDEX_FIELDS}
    for t in ('metrics', 'directories')
]

_CQL_SYNC_INDEX = "SELECT * FROM %(keyspace)s.%(table)s WHERE expr(%(table)s_idx, '{refresh:true}')"


class CassandraStratioLucene(object):
    """Cassandra SASI Query generator."""

    def __init__(self, keyspace, max_queries_per_pattern, max_metrics_per_pattern):
        """Initialize the query generator."""
        self.keyspace_metadata = keyspace
        self.max_metrics_per_pattern = max_metrics_per_pattern
        self.max_queries_per_pattern = max_queries_per_pattern

    def generate_queries(self, table, components):
        """Generate queries based on components."""
        filters = lucene.translate_to_lucene_filter(components)
        cql = "SELECT name FROM %s.%s" % (self.keyspace_metadata, table)
        if filters:
            cql += " WHERE expr(%s_idx, '%s')" % (table, filters)
        cql += " LIMIT %s" % (self.max_metrics_per_pattern + 1)

        return [cql]

    def sync_queries(self):
        """Generate queries to refresh the index."""
        for table in ('metrics', 'directories'):
            statement = c_query.SimpleStatement(
                _CQL_SYNC_INDEX % {
                    'keyspace': self.keyspace_metadata, 'table': table},
                consistency_level=c_query.ConsistencyLevel.ALL
            )
            yield statement
