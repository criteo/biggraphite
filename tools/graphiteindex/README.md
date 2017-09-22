# Big Graphite custom index for Cassandra

Custom Lucene-based index that supports selecting metric names using the
Graphite globbing syntax. It takes inspiration (and some code) from the
Cassandra implementation of SSTable-attached secondary indices (SASI) and
follows the same lifecycle.


## General ideas

## TODO

We may want/need to add rowKey token value to the Lucene index.


### Index management (Memtable + SSTables)

Necessary for querying and truncation.

- org.apache.cassandra.index.sasi.conf.DataTracker


### Memtable index


### Querying

- org.apache.cassandra.index.sasi.SASIIndex
  - SASIIndex::searcherFor
- org.apache.cassandra.index.sasi.SSTableIndex
  - SSTableIndex.DecoratedKeyFetcher is used as keyFetcher everywhere (and
    trivially makes use of `sstable.keyAt(offset)`
- org.apache.cassandra.index.sasi.plan.QueryPlan
  - QueryPlan.ResultIterator
- org.apache.cassandra.index.sasi.plan.QueryController
  - Use index tracking to run query over the whole dataset
  - QueryController::getPartition


### Additional topics

- Index truncation/drop (depends on index management)
- Misc tasks exported by the Index interface (see SASIIndex too)
- Contemplate switching to ByteBuffer instead of String all around to reduce
  garbage and copies


## Usage

### Build

`mvn install -DpackageForDeploy` will generate an uber-jar with shaded
dependencies in the `target/` directory.


### Install

Linking or copying the jar file to Cassandra's classpath (e.g. the `lib/`
directory) will make it available after a service restart.


### Use

Using your favourite way to run CQL queries against Cassandra (e.g. `cqlsh`) you
can create a custom index on a (non-primary-key, non-clustering-key) column that
contains Graphite metric paths:

```
CREATE CUSTOM INDEX my_index ON metadata(metric_path)
USING 'com.criteo.biggraphite.graphiteindex.GraphiteSASI';
```


Once the index has been built (or data has been inserted), you can query it by
using the CQL `LIKE` operator with a Graphite globbing pattern:

```
SELECT * FROM metadata WHERE metric_path LIKE 'some.*.gl{o,bb}ing.p[a-t]t?rn';
```
