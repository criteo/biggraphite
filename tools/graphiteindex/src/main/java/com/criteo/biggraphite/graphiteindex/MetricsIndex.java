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
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.NRTCachingDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.automaton.RegExp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsIndex
    implements Closeable
{
    private static final Logger logger = LoggerFactory.getLogger(MetricsIndex.class);

    // XXX(d.forest): probably useless since memtables do not have sstable offsets...
    public static void copyRamToFilesystemAndForceMerge(
        final MetricsIndex ram, final MetricsIndex fs
    )
        throws IllegalStateException, IOException
    {
        if (!ram.isInMemory()) {
            throw new IllegalStateException("Attempting to copy from a FS-based index");
        }
        if (fs.isInMemory()) {
            throw new IllegalStateException("Attempting to copy to a Memory-based index");
        }

        // Ensure Directory is up to date and references to previous generation files were dropped.
        ram.writer.commit();
        ram.searcherMgr.maybeRefreshBlocking();

        // Copy the contents of the RAM directory into FS directory
        for (String fileName : ram.directory.listAll()) {
            fs.directory.copyFrom(ram.directory, fileName, fileName, IOContext.DEFAULT);
        }

        // Force final merge of index segments into a single one.
        fs.initialize();
        fs.writer.forceMerge(1);
        fs.forceCommit();
    }

    private final String name;
    private final Optional<Path> indexPath;
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

        initialize();
    }

    private synchronized void initialize()
        throws IOException
    {
        if (reopener != null) {
            reopener.close();
        }
        if (searcherMgr != null) {
            searcherMgr.close();
        }
        if (writer != null) {
            writer.close();
        }

        IndexWriterConfig config = new IndexWriterConfig();
        // XXX(d.forest): do we want a different setRAMBufferSizeMB?

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
