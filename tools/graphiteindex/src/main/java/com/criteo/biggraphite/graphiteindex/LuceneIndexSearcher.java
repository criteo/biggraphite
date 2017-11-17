package com.criteo.biggraphite.graphiteindex;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIteratorWithLowerBound;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.sasi.plan.QueryController;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReadsListener;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.automaton.RegExp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Lucene-based Graphite metrics index.
 *
 * TODO(d.forest): provide support for globstar (**)
 *
 * TODO(d.forest): provide support for autocompletion
 */
public class LuceneIndexSearcher implements Index.Searcher, Closeable
{
    private static final Logger logger = LoggerFactory.getLogger(LuceneIndexSearcher.class);

    private final ColumnFamilyStore baseCfs;
    private final ReadCommand readCommand;
    private final ColumnDefinition column;

    public LuceneIndexSearcher(ColumnFamilyStore baseCfs, ColumnDefinition column, ReadCommand readCommand)
        throws IOException
    {
        this.baseCfs = baseCfs;
        this.readCommand = readCommand;
        this.column = column;
    }

    /**
     * TODO(p.boddu): We are searching and then creating the iterator. Incorporate searching in the iterator. This will
     * save time from unneeded results (for e.g. limit 10)
     *
     * @param executionController
     * @return
     */
    @Override
    public UnfilteredPartitionIterator search(ReadExecutionController executionController) {
        // TODO(p.boddu): Handle multiple expressions.
        ByteBuffer indexValueBb = readCommand.rowFilter().getExpressions().get(0).getIndexValue();
        String indexValue = UTF8Type.instance.compose(indexValueBb);
        BooleanQuery query = patternToQuery(indexValue);
        logger.debug("Searching Lucene index for column:{} with value:{} luceneQuery:{}",
                column.name.toString(), indexValue, query);

        /**
         * For each Live SSTable
         *  - Generates lucene index file path and opens for searching.
         *  - Searches index and stores results(List of indexPositions) in a map against the SSTable key.
         * TODO(p.boddu): Add support to read from memtable LuceneIndex. Incorporate ControlledRealTimeReopenThread
         */
        Map<SSTableReader, List<Long>> searchResultsBySSTable =
                baseCfs.getLiveSSTables().stream()
                        .map(ssTable -> {
                            String indexName = GraphiteSASI.makeIndexName(column, ssTable.descriptor.generation);
                            Path indexPath = new File(ssTable.descriptor.directory, indexName).toPath();
                            logger.debug("Searching ssTable:{} with indexPath:{}", ssTable.getFilename(), indexPath);
                            List<Long> offsets = searchOffsets(query, indexPath);
                            return Pair.of(ssTable, offsets);})
                        .collect(Collectors.toMap(t -> t.getLeft(), t -> t.getRight()));

        // TODO(p.boddu): Remove this unnecessary tranformation.
        List<Pair<SSTableReader, Long>> searchResults =
                searchResultsBySSTable.entrySet()
                        .stream()
                        .flatMap(es -> es.getValue().stream()
                                .map(offset -> Pair.of(es.getKey(), offset))
                        ).collect(Collectors.toList());

        return new LuceneResultsIterator(searchResults, readCommand);
    }


    private static class LuceneResultsIterator implements UnfilteredPartitionIterator {
        private final ReadCommand readCommand;
        private final List<Pair<SSTableReader, Long>> searchResults;
        private final Iterator<Pair<SSTableReader, Long>> searchResultsIterator;

        /**
         *
         * @param searchResults list of pair of SSTableReader and search result indexPosition in the SSTable.
         * @param readCommand associated read command.
         */
        public LuceneResultsIterator(List<Pair<SSTableReader, Long>> searchResults,
                                     ReadCommand readCommand) {
            this.searchResults = searchResults;
            this.searchResultsIterator = searchResults.iterator();
            this.readCommand = readCommand;
        }

        public boolean hasNext() {
            return searchResultsIterator.hasNext();
        }
        public UnfilteredRowIterator next() {
            try {
                Pair<SSTableReader, Long> searchResult = searchResultsIterator.next();
                SSTableReader ssTableReader = searchResult.getLeft();
                long indexPosition = searchResult.getRight();
                DecoratedKey decoratedKey = ssTableReader.keyAt(indexPosition);
                logger.debug("Fetching partition at indexPosition:{} from SSTable:{}", indexPosition, ssTableReader.getFilename());
                //UnfilteredRowIterator uri = queryController.getPartition(decoratedKey, executionController);

                UnfilteredRowIterator uri = new UnfilteredRowIteratorWithLowerBound(decoratedKey,
                        ssTableReader,
                        readCommand.clusteringIndexFilter(decoratedKey),
                        readCommand.columnFilter(),
                        readCommand.isForThrift(),
                        readCommand.nowInSec(),
                        false,
                        SSTableReadsListener.NOOP_LISTENER);
                ssTableReader.incrementReadCount();
                return uri;
            } catch(IOException e) {
                logger.error("Exception while fetching partition", e);
            }
            return null;
        }

        public void close() {}
        public boolean isForThrift()
        {
            return readCommand.isForThrift();
        }
        public CFMetaData metadata()
        {
            return readCommand.metadata();
        }
    }

    @Override
    public void close() throws IOException {}

    public List<Long> searchOffsets(BooleanQuery query, Path indexPath)
    {
        return search(query, indexPath, LuceneUtils::getOffsetFromDocument);
    }

    /**
     * TODO(p.boddu): Lot of heavy lifting here for each query. Optimize!
     *
     * @param query
     * @param indexPath
     * @param handler
     * @param <T>
     * @return
     */
    private <T> List<T> search(BooleanQuery query, Path indexPath, Function<Document, T> handler)
    {
        logger.info("{} - Searching for generated query: {}", indexPath, query);

        ArrayList<T> results = new ArrayList<>();
        Collector collector = new MetricsIndexCollector(
            doc -> results.add(handler.apply(doc))
        );

        try {
            Directory directory = FSDirectory.open(indexPath);
            SearcherManager searcherMgr = new SearcherManager(directory, new SearcherFactory());
            IndexSearcher searcher = searcherMgr.acquire();
            try {
                // TODO(d.forest): we should probably use TimeLimitingCollector
                searcher.search(query, collector);
            } catch(IOException e) {
                logger.error("{} - Cannot finish search query: {}", indexPath, query, e);
            } finally {
                searcherMgr.release(searcher);
                searcherMgr.close();
                directory.close();
            }
        } catch(IOException e) {
            logger.error("{} - Cannot acquire index searcher", indexPath, e);
        }

        return results;
    }

    /**
     * Generates a Lucene query which matches the same metrics the given Graphite
     * globbing pattern would.
     */
    private BooleanQuery patternToQuery(String pattern)
    {
        BooleanQuery.Builder queryBuilder = new BooleanQuery.Builder();

        int length = LuceneUtils.iterateOnElements(
            pattern,
            (element, depth) -> {
                // Wildcard-only fields do not filter anything.
                if (element == "*") {
                    return;
                }

                String termName = LuceneUtils.FIELD_PART_PREFIX + depth;
                Query query;
                if (element.indexOf('{') != -1 || element.indexOf('[') != -1) {
                    query = new RegexpQuery(
                        new Term(termName, graphiteToRegex(element)),
                        RegExp.NONE
                    );
                } else if (element.indexOf('*') != -1 || element.indexOf('?') != -1) {
                    query = new WildcardQuery(new Term(termName, element));
                } else {
                    query = new TermQuery(new Term(termName, element));
                }

                queryBuilder.add(query, BooleanClause.Occur.MUST);
            }
        );

        queryBuilder.add(
            IntPoint.newExactQuery(LuceneUtils.FIELD_LENGTH, length),
            BooleanClause.Occur.MUST
        );

        return queryBuilder.build();
    }

    /**
     * Naively transforms a Graphite globbing pattern into a regular expression.
     */
    private String graphiteToRegex(String query)
    {
        // TODO(d.forest): maybe make this less naive / checked?
        return query
            .replace('{', '(')
            .replace('}', ')')
            .replace(',', '|')
            .replace('?', '.')
            .replace("*", ".*?");
    }
}

