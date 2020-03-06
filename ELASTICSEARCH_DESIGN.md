# Design of the Elasticsearch backend

This document describes data model for storing and retrieving metadata and timeseries data to/from
Elasticsearch.


# Indices

## Metadata

Metric is received as a string (ex: `foo.bar.baz`). This string is the _name_ of the metric
and is stored as-is in a text field.

Metric names are split per component: from `p0` to `p63` (default value). For `foo.bar.baz`,
you would get:

```json
{
  "name": "foo.bar.baz",
  "p0": "foo",
  "p1": "bar",
  "p2": "baz"
}
```

The _depth_ of a metric is defined by the number of components minus 1 (this matches the last
non-empty `p{i}`).

Examples:

* `depth(foo) = 0`
* `depth(foo.bar.baz) = 2`

Every metric has its own configuration, stored in the `config` field. This configuration
contains:

* the aggregator;
* the retention policy;
* the Carbon `xFilesFactor`.

The metadata contain only metrics: directories are not stored in Elasticsearch.
They can be deduced from a prefix and a lower bound depth: any existing path that is not a leaf is
a directory.

Index mapping:
```json
"mappings": {
    "_doc": {
        "properties": {
            "depth": {"type": "long"},
            "created_on": {"type": "date"},
            "read_on": {"type": "date"},
            "updated_on": {"type": "date"},
            "name": {
                "type": "keyword",
                "ignore_above": 1024,
            },
            "uuid": {
                "type": "keyword",
            },
            "config": {
                "type": "object"
            },
        },
        "dynamic_templates": [
            {
                "strings_as_keywords": {
                    "match": "p*",
                    "match_mapping_type": "string",
                    "mapping": {
                        "type": "keyword",
                        "ignore_above": 256,
                        "ignore_malformed": True,
                    }
                }
            }
        ]
    },
}
```

### Metrics

Example of a metric search:
```
# criteo.{rtb,cas}.*.*.*.*.*.*.*.??_???.web-rtb*
GET biggraphite-metrics*/_search
{
   "_source": ["name", "uuid", "created_on", "read_on", "updated_on", "config"],
  "query": {
    "bool": {
      "filter": [
        {
          "term": {
            "p0": "criteo"
          }
        },
        {
          "terms": {
            "p1": [
              "rtb",
              "cas"
            ]
          }
        },
        {
          "wildcard": {
            "p10": "web-rtb*"
          }
        },
        {
          "regexp": {
            "p9": "([a-z]{2})_([a-z0-9]{3})"
          }
        },
        {
          "term": {
            "depth": 10
          }
        }
      ]
    }
  }
}
```

### Directories

Directories are paths that have at least one sub-element. For metric `foo.bar.baz`, `foo` and
`foo.bar` are directories.

To get directories under a given prefix, we list metrics that are under that directory (using the
depth) and aggregate the first children under that prefix.

Example to list directories under `criteo`:
```
# criteo.*
GET biggraphite_metrics*/_search
{
  "size": "0",
  "query": {
    "bool": {
      "filter": [
        {
          "term": {
            "p0": "criteo"
          }
        },
        {
          "range": {
            "depth": {
              "gte": 2
            }
          }
        }
      ]
    }
  },
  "aggs": {
    "distinct_dirs": {
      "terms": {
        "field": "p1",
        "size": 10000,
        "min_doc_count": 1
      }
    }
  }
}
```

Example to list directories under `criteo.*.foo.*`:
```
# criteo.foo.bar*
GET biggraphite_metrics*/_search
{
  "size": "0",
  "query": {
    "bool": {
      "filter": [
        {
          "term": {
            "p0": "criteo",
          }
        },
        {
          "term": {
            "p2": "foo",
          }
        },
        {
          "range": {
            "depth": {
              "gte": 4
            }
          }
        }
      ]
    }
  },
  "aggs": {
    "distinct_dirs": {
      "terms": {
        "field": "p3",
        "size": 10000,
        "min_doc_count": 1
      }
    }
  }
}
```


> Note: globstar queries (ex: `foo.bar.**`) are not yet supported for directories.

### Retention

Indices are named by date. Default pattern is `biggraphite_metrics_{year}-{month}-{day}`.
When a metric point is written, an entry for this metric is created or updated in the current index.


For performance reasons, this action is not performed at every write: a configurable delay prevents
from updating the documents before a certain time as elapsed.

```text
write value ---------------------------------------------------
                |                      |  |           |        |
                V                      V  V           V        V
    ===================================================================> time
                ^                                     ^
                |                                     |
                | < ------- update TTL ------- >      | < ------- update TTL ...
                |                                     |
update metadata --------------------------------------
```

To remove obsolete metrics (_ie_ not written since a given date), remove the indices for prior
dates.

This time-based naming allows to get the metric metadata as they were defined at the
time of the queried time range. In the example below, we have four versions of the
metric (`A`, `B`, `C` and `D`). The Elasticsearch driver will always fetch the most
recent version available for the time range. Here, it would be `C`.

If no time range is provided, the
driver will use the latest version of the metric metadata.


```text
   --------------------------------------------------- metadata update
  |                  |    |                |
  V                  V    V                V
--A------------------B----C----------------D------- > time
      |                            |
      | < -- query time range -- > |
```

## Data

_Unsupported for now._

## Directory index

Another way to handle directory search is by adding another index for directories with this mapping: 
```json
{
"_doc": {
"properties": {
            "depth": { 
                "type": "long"
            },

            "name": {
                "type": "keyword",
                "ignore_above": 1024
            },
            "uuid": {
                "type": "keyword"
            },
            "parent": {
                "type": "keyword"
            }
        },
			"dynamic_templates": [
            {
                "strings_as_keywords": {
                    "match": "p*",
                    "match_mapping_type": "string",
                    "mapping": {
                        "type": "keyword",
                        "ignore_above": 256,
                        "ignore_malformed": true
                    }
                }
            }
        ]
	}
}
```
The current version of biggraphite only support reading from such an index. 

When using it, the search for directory would look like this: 

```json
# root.*.prod.*
GET biggraphite_metrics*/_search
{
  "size": "0",
  "query": {
    "bool": {
      "filter": [
        {
          "term": {
            "p0": "root"
          }
        },
        {
          "term": {
            "p2": "prod"
          }
        },
        {
          "term": {
            "depth": "3"
          }
        },
      ]
    }
  },
  "aggs": {
    "distinct_dirs": {
      "terms": {
        "field": "p1",
        "size": 10000,
        "min_doc_count": 1
      }
    }
  }
}
```
This narrows the search, especially if the directory is close from the root. We still perform aggregation to prevent duplicate accross different indices that would include the directory
If the directory parent is not glob, we can optimise by searching on parent field ([https://www.elastic.co/guide/en/elasticsearch/reference/6.8/tune-for-search-speed.html#_search_as_few_fields_as_possible](url))