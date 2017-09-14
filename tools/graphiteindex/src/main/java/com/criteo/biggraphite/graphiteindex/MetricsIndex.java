package com.criteo.biggraphite.graphiteindex;

import java.io.Closeable;
import java.io.IOException;
import java.lang.IllegalStateException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BooleanQuery.Builder;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.automaton.RegExp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.lucene.store.IOContext;

public class MetricsIndex implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(MetricsIndex.class);

    private Optional<Path> indexPath;
    private Directory directory;
    private IndexWriter writer;
    private DirectoryReader reader;
    private IndexSearcher searcher;

    public MetricsIndex()
        throws IOException
    {
        initialize(new RAMDirectory());
    }

    public MetricsIndex(Path indexPath)
        throws IOException
    {
        initialize(FSDirectory.open(indexPath));
    }

    private void initialize(Directory directory)
        throws IOException
    {
        IndexWriterConfig config = new IndexWriterConfig();

        this.directory = directory;
        this.writer = new IndexWriter(directory, config);
        this.reader = DirectoryReader.open(writer);
        this.searcher = new IndexSearcher(reader);
    }

    @Override
    public void close()
        throws IOException
    {
        reader.close();
        writer.close();
        directory.close();
    }

    public boolean isInMemory() {
        return !indexPath.isPresent();
    }

    public synchronized void makePersistent(Path indexPath)
        throws IllegalStateException, IOException
    {
        if (!(directory instanceof RAMDirectory)) {
            throw new IllegalStateException("Attempting to make a non-volatile index persistent");
        }

        commit();
        reader.close();

        // Force in-memory merge of all segments into one before copying to disk.
        writer.forceMerge(1);
        writer.close();

        Directory onDiskDirectory = FSDirectory.open(indexPath);
        for (String file : this.directory.listAll()) {
            onDiskDirectory.copyFrom(this.directory, file, file, IOContext.DEFAULT);
        }

        initialize(onDiskDirectory);
    }

    public synchronized void commit()
    {
        logger.debug("Committing");

        try {
            writer.commit();
        } catch(IOException e) {
            logger.error("Cannot commit index", e);
        }

        try {
            DirectoryReader newReader = DirectoryReader.openIfChanged(reader);
            if (newReader != null) {
                reader = newReader;
                searcher = new IndexSearcher(reader);
            }
        } catch(IOException e) {
            logger.error("Cannot re-open index reader", e);
        }
    }

    public void insert(String path)
    {
        logger.debug("Inserting '{}'", path);

        Document doc = MetricPath.toDocument(path);

        try {
            writer.addDocument(doc);
        } catch(IOException e) {
            logger.error("Cannot insert metric in index", e);
        }
    }

    public List<String> search(String pattern)
    {
        BooleanQuery query = patternToQuery(pattern);
        logger.debug("Searching for '{}', generated query: {}", pattern, query);

        ArrayList<String> results = new ArrayList<>();
        Collector collector = new MetricsIndexCollector(
            doc -> results.add(MetricPath.fromDocument(doc))
        );

        try {
            // TODO(d.forest): we should probably use TimeLimitingCollector
            searcher.search(query, collector);
        } catch(IOException e) {
            logger.error("Cannot finish search query", e);
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
