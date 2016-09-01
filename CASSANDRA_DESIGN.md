# Design of the Cassandra backend

This document describes data model for storing and retrieving timeseries data to/from Cassandra.


# Keyspaces

Two keyspaces are used, one for data and one for metadata.
We recommend using the following settings: [schema.cql](share/schema.cql).


# Data tables
For the full CQL please see [cassandra.py](biggraphite/drivers/cassandra.py).


## Primary key

All rows in data table describe have the metric name and starting timestamp as part of their primary key, use the timestamp offset as the column key for each value.

We group related timestamps (`_row_size_ms`) in the same row described by `time_start_ms`, using `offset` to describe a delta from it. The unit of this delta is the precision of the current stage/table (`timestamp_ms = time_start_ms + offset * stage.precision_ms`).

This saves space in two ways:

- No need to repeatedly store metric IDs for each entry
- The relative offset is only 4 bytes, while timestamps are 8 bytes


## Expiry of data

TTL-based expiration is implemented using Cassandra's [DateTieredCompactionStrategy](http://www.datastax.com/dev/blog/datetieredcompactionstrategy). We therefore need a compaction configuration for each downsampling configuration.

As compaction configurations are per Cassandra table, we need to have one table for each resolution (or “stage”) of the retention policies.
For example the policy "store 60 points with a resolution of 60 seconds, then 24 points with a resolution of 1 hour" requires two tables with their own configuration: `datapoints_60p_60s` and `datapoints_24p_3600s`.


# Metadata tables

Metadata entries are currently stored in two different tables, one for directories (path nodes) and one for metrics (leaves).

Advantages of having separate two tables (**current implementation**):

- We expect them to contain increasingly different information (i.e. quotas on directories vs. whisper-header-like on metrics) and the semantics of single merged entries would become complex
- We suspect that implementing quotas or cleaning up empty directories will be easier if we can iterate on directories without going through all the individual metrics

Advantages of using one table:

- We could use Cassandra's batch statements to update metadata at the same time as parent entries
- We could still build a materialized view of directories if we actually need the previously described performance benefits

None of the above is a strong decision factor, but we are keeping then separate because merging things is generally easier than untangling them.


## Metrics (leaves) metadata table

A metric's metadata contains:
- Name
- Map of configuration keys / JSON values

In order to resolve globs, we store metrics in a table that among other things contain 64 columns: one for each path component. The number of components is configurable, and we chose 64 as an arbitrary value that is good enough for the use cases we have in mind.

The metric `a.b.c` is therefore indexed as:
```python
name="a.b.c",
config=...,
component_0="a",
component_1="b",
component_2="c",
component_3="__END__",
component_4=NULL,
[...],
component_63=NULL
```

An SSTable-Attached Secondary Index (SASI, [see the official documentation for details](https://github.com/apache/cassandra/blob/trunk/doc/SASI.md)) is declared on each of the component columns.

We use SASI because it is able to process multi-column queries by intersection (as opposed to a brute-force approach relying on Cassandra's `ALLOW FILTERING`). It indexes tokens using one B+Tree per indexed column in the original SSTable. After finding the keys (tokens) in the B+Trees, SASI picks the column with the least results and merges the results together. If one column is 100 times larger than the others it will use multiple lookups, otherwise it just uses a merge join.

As SASI's support for LIKE queries is limited to finding substrings, we actually look for `a.*.c` when asked for `a.x*z.c` and then do client-side filtering.


## Directories (path nodes) metadata table

Similar to the metrics table, we create entries for each node along in path to each metric in a directory table. It is used to implement the 'metric' API in graphite.

Directories are implicitly created before metrics by the `Accessor.create_metric()` method. Because of this (or because of deleted/expired metrics), it is possible to end up with an empty directory (if the program crashes). A cleanup job will be needed if they accumulate for too long.
