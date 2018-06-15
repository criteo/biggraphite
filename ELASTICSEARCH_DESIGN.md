# Design of the ElasticSearch backend

This document describes data model for storing and retrieving metadata and timeseries data to/from ElasticSearch


# Indices

## Metadata

### Metrics

Getting metrics
```

# criteo.rtb.*.*.*.*.*.*.*.??_???.web-rtb*
GET biggraphite/_search
{
   "_source": ["name", "uuid", "created_on", "read_on", "updated_on", "config"],
  "size": "9999",
  "query": {
    "bool": {
      "filter": [
        {
          "term": {
            "p0.keyword": "criteo"
          }
        },
        {
          "terms": {
            "p1.keyword": [
              "rtb",
              "cas"
            ]
          }
        },
        {
          "wildcard": {
            "p10.keyword": "web-rtb*"
          }
        },
        {
          "regexp": {
            "p9.keyword": "([a-z]{2})_([a-z0-9]{3})"
          }
        },
        {
          "term": {
            "length": 10
          }
        }
      ]
    }
  }
}
```

### Directories

Getting directories without a dedicated index.
```
GET biggraphite_metrics/_search
{
  "size": "0",
  "query": {
    "bool": {
      "filter": [
        {
          "term": {
            "p0.keyword": "criteo"
          }
        }
      ],
      "must": [
        {
          "range": {
            "length": {
              "gte": 2
            }
          }
        }
      ]
    }
  },
  "aggs": {
    "distinct_p1": {
      "terms": {
        "field": "p1.keyword",
        "size": 999,
        "min_doc_count": 1
      }
    }
  }
}

GET biggraphite_directories/_search
{
  "query": {
    "bool": {
      "filter": [
        {
          "term": {
            "p0.keyword": "criteo"
          }
        },
        {
          "term": {
            "length": 1
          }
        }
      ]
    }
  }
}

GET biggraphite/_search
{
  "size": "0",
  "query": {
    "match": {
      "p0.keyword": "criteo"
    }
  },
  "aggs": {
    "distinct_p1": {
      "cardinality": {
        "field": "p1.keyword"
      }
    }
  }
}
```


## Data
