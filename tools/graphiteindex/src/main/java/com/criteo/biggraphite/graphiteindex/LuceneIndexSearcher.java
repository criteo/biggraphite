package com.criteo.biggraphite.graphiteindex;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.index.Index;
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
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
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

    private final Path indexPath;
    private final ReadCommand readCommand;
    private Directory directory;
    private SearcherManager searcherMgr;
    private ControlledRealTimeReopenThread<IndexSearcher> reopener;

    /**
     * TODO(p.boddu): Add support to read from in-memory LuceneIndex. Including ControlledRealTimeReopenThread
     */
    public LuceneIndexSearcher(Path indexPath, ReadCommand readCommand)
        throws IOException
    {
        this.indexPath = indexPath;
        this.readCommand = readCommand;
        //this.directory = FSDirectory.open(indexPath);
        //this.searcherMgr = new SearcherManager(this.directory, new SearcherFactory());
    }

    @Override
    public UnfilteredPartitionIterator search(ReadExecutionController executionController) {
        return new UnfilteredPartitionIterator() {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public UnfilteredRowIterator next() {
                return null;
            }

            @Override
            public void close() {

            }

            @Override public boolean isForThrift() { return readCommand.isForThrift(); }
            @Override public CFMetaData metadata()
            {
                return readCommand.metadata();
            }
        };
    }

    @Override
    public void close()
        throws IOException
    {
        reopener.close();
        searcherMgr.close();
        directory.close();
    }

    /*
    public boolean isInMemory() {
        return !indexPath.isPresent();
    }
    */

    public List<Long> searchOffsets(String pattern)
    {
        return search(pattern, LuceneUtils::getOffsetFromDocument);
    }

    public List<Pair<String, Long>> searchPaths(String pattern)
    {
        return search(
            pattern,
            doc -> Pair.of(
                LuceneUtils.getPathFromDocument(doc),
                LuceneUtils.getOffsetFromDocument(doc)
            )
        );
    }

    private <T> List<T> search(String pattern, Function<Document, T> handler)
    {
        BooleanQuery query = patternToQuery(pattern);
        logger.info("{} - Searching for '{}', generated query: {}", indexPath, pattern, query);

        ArrayList<T> results = new ArrayList<>();
        Collector collector = new MetricsIndexCollector(
            doc -> results.add(handler.apply(doc))
        );

        try {
            IndexSearcher searcher = searcherMgr.acquire();
            try {
                // TODO(d.forest): we should probably use TimeLimitingCollector
                searcher.search(query, collector);
            } catch(IOException e) {
                logger.error("{} - Cannot finish search query: {}", indexPath, query, e);
            } finally {
                searcherMgr.release(searcher);
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
