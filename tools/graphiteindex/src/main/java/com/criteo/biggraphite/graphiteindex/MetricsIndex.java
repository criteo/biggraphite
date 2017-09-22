package com.criteo.biggraphite.graphiteindex;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BooleanQuery.Builder;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.ControlledRealTimeReopenThread;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.NRTCachingDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.automaton.RegExp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Lucene-based Graphite metrics index.
 *
 * TODO(d.forest): switch from String to ByteBuffer to minimize extra garbage/copies?
 *
 * TODO(d.forest): this could probably be split in three stages:
 * - RAM: for memtable index (with NRT)
 * - FS-RW: for read-write on-disk index, during sstable write/merge (with NRT)
 * - FS-RO: for read-only on-disk index consultation (without NRT, without writer)
 */
public class MetricsIndex
    implements Closeable
{
    private static final Logger logger = LoggerFactory.getLogger(MetricsIndex.class);

    public final String name;
    public final Optional<Path> indexPath;

    private Directory directory;
    private IndexWriter writer;
    private SearcherManager searcherMgr;
    private ControlledRealTimeReopenThread<IndexSearcher> reopener;

    public MetricsIndex(String name)
        throws IOException
    {
        this(name, Optional.empty());
    }

    public MetricsIndex(String name, Optional<Path> indexPath)
        throws IOException
    {
        IndexWriterConfig config = new IndexWriterConfig();
        // XXX(d.forest): do we want a different setRAMBufferSizeMB?

        Directory directory;
        if (indexPath.isPresent()) {
            directory = FSDirectory.open(indexPath.get());

            // Adds a RAMDirectory cache, which helps with low-frequently updates and
            // high-frequency re-open, which is likely to be the case for Graphite metrics.
            // 8MB max segment size, 64MB max cached bytes.
            directory = new NRTCachingDirectory(directory, 8, 64);
        } else {
            directory = new RAMDirectory();
        }

        this.name = name;
        this.indexPath = indexPath;
        this.directory = directory;
        this.writer = new IndexWriter(directory, config);
        this.searcherMgr = new SearcherManager(writer, new SearcherFactory());

        // Enforce changes becoming visible to search within 10 to 30 seconds.
        this.reopener = new ControlledRealTimeReopenThread<>(this.writer, searcherMgr, 30, 10);
        this.reopener.start();
    }

    @Override
    public void close()
        throws IOException
    {
        reopener.close();
        searcherMgr.close();
        writer.close();
        directory.close();
    }

    public boolean isInMemory() {
        return !indexPath.isPresent();
    }

    public synchronized void forceCommit()
    {
        logger.debug("{} - Forcing commit and refreshing searchers", name);

        try {
            writer.commit();
            searcherMgr.maybeRefreshBlocking();
        } catch(IOException e) {
            logger.error("{} - Cannot commit or refresh searchers", name, e);
        }
    }

    /**
     * Forces Lucene to merge index segments, then commits to drop old segments.
     * It is currently used as a final step in index construction to compress everything
     * into a single index segment.
     * This can be slow, and should not be used frequently (currently once per index).
     */
    public synchronized void forceMerge()
    {
        logger.debug("{} - Forcing merge of index segments", name);

        try {
            writer.forceMerge(1);
            forceCommit();
        } catch(IOException e) {
            logger.error("{} - Cannot force merge of index segments", name, e);
        }
    }

    public void insert(String path)
    {
        logger.trace("{} - Inserting '{}'", name, path);

        Document doc = MetricPath.toDocument(path);

        try {
            writer.addDocument(doc);
        } catch(IOException e) {
            logger.error("{} - Cannot insert metric in index", name, e);
        }
    }

    public void insert(String path, long offset)
    {
        logger.trace("{} - Inserting '{}' with offset {}", name, path);

        Document doc = MetricPath.toDocument(path, offset);

        try {
            writer.addDocument(doc);
        } catch(IOException e) {
            logger.error("{} - Cannot insert metric in index", name, e);
        }
    }

    public List<Long> searchOffsets(String pattern)
    {
        return search(pattern, MetricPath::getOffsetFromDocument);
    }

    public List<Pair<String, Long>> searchPaths(String pattern)
    {
        return search(
            pattern,
            doc -> Pair.of(
                MetricPath.getPathFromDocument(doc),
                MetricPath.getOffsetFromDocument(doc)
            )
        );
    }

    private <T> List<T> search(String pattern, Function<Document, T> handler)
    {
        BooleanQuery query = patternToQuery(pattern);
        logger.trace("{} - Searching for '{}', generated query: {}", name, pattern, query);

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
                logger.error("{} - Cannot finish search query", name, e);
            } finally {
                searcherMgr.release(searcher);
            }
        } catch(IOException e) {
            logger.error("{} - Cannot acquire index searcher", name, e);
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

        int length = MetricPath.iterateOnElements(
            pattern,
            (element, depth) -> {
                // Wildcard-only fields do not filter anything
                // TODO(d.forest): detect several wildcards?
                if (element == "*") {
                    return;
                }

                String termName = MetricPath.FIELD_PART_PREFIX + depth;
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
            IntPoint.newExactQuery(MetricPath.FIELD_LENGTH, length),
            BooleanClause.Occur.MUST
        );

        return queryBuilder.build();
    }

    /**
     * Naively transforms a Graphite globbing pattern into a regular expression.
     */
    private String graphiteToRegex(String query)
    {
        return query
            .replace('{', '(')
            .replace('}', ')')
            .replace(',', '|')
            .replace('?', '.')
            .replace("*", ".*?");
    }
}
