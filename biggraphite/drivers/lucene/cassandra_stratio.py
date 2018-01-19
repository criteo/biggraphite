import lucene
import cassandra
import os

_COMPONENTS_INDEX_MAX_LEN = int(os.environ.get('BG_COMPONENTS_INDEX_MAX_LEN',os.environ.get('BG_COMPONENTS_MAX_LEN', 64)))

_CQL_PATH_COMPONENTS_INDEX_FIELDS = ",\n".join(
    "        component_%d : {type: \"string\"}" % n for n in range(_COMPONENTS_INDEX_MAX_LEN)
)

CQL_CREATE_INDICES = [
    "CREATE CUSTOM INDEX IF NOT EXISTS %(table)s_idx ON \"%%(keyspace)s\".%(table)s()"
    "  USING 'com.stratio.cassandra.lucene.Index' WITH OPTIONS = {"
    "    'schema': '{"
    "      fields: {"
    "        parent : {type: \"string\"},"
    "%(components)s"
    "      }"
    "    }'"
    "  };"  % {"table": t, "components": _CQL_PATH_COMPONENTS_INDEX_FIELDS}
    for t in ('metrics', 'directories')
]

_CQL_SELECT_NAME_SEARCH_QUERY = """SELECT name
FROM {keyspace}.{table}
WHERE expr({table}_idx, '{lucene_filter}')
LIMIT {limit}"""


def generate_query(keyspace, table, glob, max_metrics_per_pattern):
    return _CQL_SELECT_NAME_SEARCH_QUERY.format(
            keyspace=keyspace,
            table=table,
            lucene_filter=lucene.translate_glob_to_lucene_filter(glob),
            limit=max_metrics_per_pattern
            )
