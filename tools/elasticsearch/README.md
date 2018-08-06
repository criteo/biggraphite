# Import metadata into Elasticsearch

If you already have a TSDB, you may want to import your current metrics into Elasticsearch before
using BigGraphite backed by Elasticsearch for metadata. 

## Step 1: export your metadata as a CSV

Expected format is:
```csv
metric_name;config;created_on;uid;read_on;updated_on 
```

Example:
```csv
foo.bar,"{'aggregator': 'average', 'carbon_xfilesfactor': '0.500000', 'retention': '11520*60s:720*3600s:365*86400s'}",cd580bd0-96fa-11e8-a401-a1267081804f,6385d708-7643-59d9-9c72-cb0162675f26,8bba4902-971c-11e8-a401-a1267081804f,cd580bd1-96fa-11e8-a401-a1267081804f
```

If you are migrating from a BigGraphite Cassandra backend for metadata to an Elasticsearch backend, 
you can export them using the following command from `cqlsh` on the metadata keyspace:

```cql
COPY biggraphite_metadata.metrics_metadata (name, config, created_on, id, read_on, updated_on) TO 'metrics_metadata.csv';  
``` 

## Step 2: import the CSV into Elasticsearch

Use `import-metrics.py` script:

```
usage: import-metrics.py [-h] [--max_workers MAX_WORKERS]
                         [--username USERNAME] [--password PASSWORD]
                         [--cluster CLUSTER] [--sniff] [--port PORT]
                         [--cleanup] [--prefix_labelled PREFIX_LABELLED]
                         --input INPUT
```

