package com.criteo.biggraphite.graphiteindex;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIteratorWithLowerBound;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReadsListener;
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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

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

    @Override
    public UnfilteredPartitionIterator search(ReadExecutionController executionController) {
        /*
         TODO(p.boddu): Handle multiple expressions like "secondaryIndexColumn1='pattern1' AND secondaryIndexColumn2='pattern2'"
            It doesn't apply to Biggraphite's problem at the moment.
            It applies to general SASI index search use-case.
          */
        ByteBuffer indexValueBb = readCommand.rowFilter().getExpressions().get(0).getIndexValue();
        String indexValue = UTF8Type.instance.compose(indexValueBb);
        BooleanQuery query = patternToQuery(indexValue);
        logger.debug("Searching Lucene index for column:{} with value:{} luceneQuery:{}",
                column.name.toString(), indexValue, query);

        // TODO(p.boddu): Add support to read from memtable LuceneIndex. Incorporate ControlledRealTimeReopenThread
        return new LuceneResultsIterator(baseCfs.getLiveSSTables(), readCommand, query, column);
    }


    private static class LuceneResultsIterator implements UnfilteredPartitionIterator {
        private final Iterator<SSTableReader> ssTables;
        private final ReadCommand readCommand;
        private final BooleanQuery query;
        private final ColumnDefinition column;
        private SSTableReader currentSSTable;
        private Iterator<Long> currentResults;

        public LuceneResultsIterator(Set<SSTableReader> ssTables,
                                     ReadCommand readCommand,
                                     BooleanQuery luceneQuery,
                                     ColumnDefinition column) {
            this.ssTables = ssTables.iterator();
            this.readCommand = readCommand;
            this.query = luceneQuery;
            this.column = column;
        }

        public boolean hasNext() {
            if(currentResults != null && currentResults.hasNext()) {
                return true;
            } else {
                // Initialize new search results
                while(ssTables.hasNext()) {
                    currentSSTable = ssTables.next();
                    String indexName = GraphiteSASI.makeIndexName(column, currentSSTable.descriptor.generation);
                    Path indexPath = new File(currentSSTable.descriptor.directory, indexName).toPath();
                    logger.debug("Searching ssTable:{} with indexPath:{}", currentSSTable.getFilename(), indexPath);
                    List<Long> offsets = searchOffsets(query, indexPath);
                    currentResults = offsets.iterator();
                    if(currentResults.hasNext()) {
                        return true;
                    }
                }
                return false;
            }
        }

        public UnfilteredRowIterator next() {
            try {
                long indexPosition = currentResults.next();
                DecoratedKey decoratedKey = currentSSTable.keyAt(indexPosition);
                logger.debug("Fetching partition at indexPosition:{} from SSTable:{}",
                        indexPosition, currentSSTable.getFilename());

                UnfilteredRowIterator uri = new UnfilteredRowIteratorWithLowerBound(decoratedKey,
                        currentSSTable,
                        readCommand.clusteringIndexFilter(decoratedKey),
                        readCommand.columnFilter(),
                        readCommand.isForThrift(),
                        readCommand.nowInSec(),
                        false,
                        SSTableReadsListener.NOOP_LISTENER);
                currentSSTable.incrementReadCount();
                return uri;
            } catch(IOException e) {
                logger.error("Exception while fetching partition", e);
            }
            return null;
        }

        public List<Long> searchOffsets(BooleanQuery query, Path indexPath)
        {
            return search(query, indexPath, LuceneUtils::getOffsetFromDocument);
        }

        /**
         * SearcherManager facilitates safely sharing IndexSearcher instances across multiple thread.
         * TODO(p.boddu): Confirm that calling FSDirectory.open() on every search call doesn't hog system resources.
         */
        private <T> List<T> search(BooleanQuery query, Path indexPath, Function<Document, T> handler)
        {
            logger.info("{} - Searching for generated query: {}", indexPath, query);

            ArrayList<T> results = new ArrayList<>();
            Collector collector = new LuceneSearchResultsCollector(
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

