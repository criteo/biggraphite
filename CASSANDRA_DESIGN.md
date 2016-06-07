# Cassandra backend
This document describes how we modeled a TSDB using Cassandra.

------

## data tables
For the full SQL please see [cassandra.py](biggraphite/drivers/cassandra.py).

### Primary key
All rows in data table describe have time and metric UUID as their primary key, and values as columns.<br />
We group related timestamps (`_ROW_SIZE_MS`) in the same row described by `time_start_ms` and using `time_offset_ms` to describe  a delta from it. This saves space in two ways:
 - No need to repeat metric IDs on each row.
 - The relative time offset is 4 bytes only when a timestamp would be 8.

### Expiry of data
TTLs take space and are only applied on compaction. Tweaking compaction is tricky and that makes it hard to recover space in emergency by expiring data early.<br />
An alternative would be to have one table per day of expiry, but for 10 years of retentions that would be about 3650 tables.<br />
So what we do is that we round up retention policy to the next power of two and create a table per such retention policy.

At any time we have 11 tables, corresponding to 1 TTL: 4 days, 8 days, 16 days, ..., 2048 days.
We write and read a timestamp in the table that best fit. If we assume there is about the same amount of data week-over-week, we store **50% more than instant deletion**.

**Example:**
On the 28th of December 2015, the following 10 tables exist: "2016-01-01", "2016-01-09", "2016-01-25", "2016-02-24", ..., "2019-08-11".

| TTL       |       |  E    | X     | P     | I     | R     | Y     |       |
|--:	    |:-:	|:-:	|:-:	|:-:	|:-:	|:-:	|:-:	|:-:	|
| **8d** 	|Jan 01 |Jan 09 |       |       |       |       |       |       |
| **16d**	|Jan 09 |Jan 09 |Jan 25 |Jan 25 |       |       |       | *...* |
| **32d**   |Jan 25 |Jan 25 |Jan 25 |Jan 25 |Feb 24 |Feb 24 |Feb 24 | *...* |
| *...*     | *...* | *...* | *...* | *...* | *...* | *...* | *...* | *...* |
| **2048d**	|2019   |2019   |2019   |2019   |2019   |2019   |2019   | *...* |

So a point written on 2015-12-28 with a desired TTL of 30 days should expire on 2016-01-27 but instead will expire on 2016-02-24 .

------

## metadata tables
Metadata are stored in two different tables, one for metrics and one for directories.

Advantages of having separate two tables (current solution):
  - We expect them to end up with increasingly different metadata (quotas on directories and
    whisper-header-like on metrics) and the semantic of a merged entry will become complex
  - We suspect that implementing quota or cleaning of empty directories will be easier if
    we can iterate on directories without filtering all metrics

Advantages of using one table:
  - We get batch transactions to update metadata at the same time as parent entries
  - We could still build a materialized view of directories only to get performance
    benefits above

None of the points above is a strong decision factor, but merging things later is generally easier than untangling them so we kept them separate for now.


### "metrics" metadata table
**Metrics metadata are: a name, a map of config keys to json values and a UUID.**

To resolve globs, we store metrics in a table that among other things contain 64 columns, one for each path component.

The metric `a.b.c` is therefore indexed as:
```python
   name="a.b.c", config=...,
   component_0="a", component_1="b", component_2="c", component_3="__END__",
   component_4=NULL, component_5=NULL, ..., component_63=NULL
```
A SASI Index is declared on each of the component columns.<br />
We use SASI because it knows to process multi-column queries without filtering. The way it works is by indexing tokens in a B+Tree for each column of each sstable. After finding the right places in the B+Trees, SASI picks the column with the least results, and merge the result together. It uses multiple lookups if one column is 100 times bigger than the other, otherwise it uses a merge join.

SASI support for LIKE queries is limited to finding substrings. So when resolving a query like `a.x*z.c` we query for `a.*.c` and then do client-side filtering. <br />
[See here for more details on SASI](https://github.com/apache/cassandra/blob/trunk/doc/SASI.md)

### "directories" metadata table
Similar to the metrics table, we create for parents of all metrics an entry in a directory table. It is used to implement the 'metric' API in graphite.

Directories are implicitely created before metrics by the Accessor.create_metric() function. Because of this, it is possible to end up with an empty directory (if the program crashes). A cleaner job will be needed if they accumulate for too long.
