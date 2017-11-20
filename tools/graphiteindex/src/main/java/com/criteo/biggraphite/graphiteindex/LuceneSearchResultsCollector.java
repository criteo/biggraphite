package com.criteo.biggraphite.graphiteindex;

import java.io.IOException;
import java.util.function.Consumer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorer;

public class LuceneSearchResultsCollector
    implements Collector, LeafCollector
{
    private final Consumer<Document> consumer;
    private LeafReader reader;

    public LuceneSearchResultsCollector(Consumer<Document> consumer)
    {
        this.consumer = consumer;
        this.reader = null;
    }

    @Override public boolean needsScores()
    {
        return false;
    }

    @Override public void setScorer(Scorer scorer)
    {
    }

    @Override public LeafCollector getLeafCollector(LeafReaderContext context)
    {
        this.reader = context.reader();
        return this;
    }

    @Override public void collect(int doc)
        throws IOException
    {
        consumer.accept(reader.document(doc));
    }
}
