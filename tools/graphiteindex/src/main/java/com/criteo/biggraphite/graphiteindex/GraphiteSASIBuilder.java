package com.criteo.biggraphite.graphiteindex;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;
import java.util.SortedSet;
import java.util.UUID;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.db.compaction.CompactionInterruptedException;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.index.SecondaryIndexBuilder;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.sstable.KeyIterator;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphiteSASIBuilder
    extends SecondaryIndexBuilder
{
    private static final Logger logger = LoggerFactory.getLogger(GraphiteSASIBuilder.class);

    private final ColumnFamilyStore cfs;
    private final ColumnDefinition column;
    private final SortedSet<SSTableReader> sstables;

    private final UUID compactionId;
    private final long totalBytes;
    private long processedBytes;

    public GraphiteSASIBuilder(
        ColumnFamilyStore cfs, ColumnDefinition column, SortedSet<SSTableReader> sstables
    )
    {
        long totalBytes = 0;
        for (SSTableReader reader : sstables) {
            totalBytes += reader.getIndexFile().dataLength();
        }

        this.cfs = cfs;
        this.column = column;
        this.sstables = sstables;

        this.compactionId = UUIDGen.getTimeUUID();
        this.totalBytes = totalBytes;
        this.processedBytes = 0;
    }

    @Override public CompactionInfo getCompactionInfo()
    {
        return new CompactionInfo(
            cfs.metadata,
            OperationType.INDEX_BUILD,
            processedBytes,
            totalBytes,
            compactionId
        );
    }

    @Override public void build()
    {
        sstables.forEach(this::buildIndex);
    }

    protected void buildIndex(SSTableReader sstable)
    {
        try (
            RandomAccessReader dataFile = sstable.openDataReader();
            KeyIterator keys = new KeyIterator(sstable.descriptor, cfs.metadata);
        ) {
            String indexName = GraphiteSASI.makeIndexName(column, sstable.descriptor.generation);
            logger.debug("Building index {}", indexName);

            Path indexPath = new File(sstable.descriptor.directory, indexName).toPath();
            LuceneIndex index = new LuceneIndex(indexName, Optional.of(indexPath));

            PerSSTableIndexWriter indexWriter = new PerSSTableIndexWriter(
                sstable.descriptor, OperationType.COMPACTION, column, index
            );

            long previousKeyPosition = 0;
            while (keys.hasNext()) {
                if (isStopRequested()) {
                    logger.trace("Stop requested during build for {}", indexName);
                    throw new CompactionInterruptedException(getCompactionInfo());
                }

                final DecoratedKey key = keys.next();
                final long keyPosition = keys.getKeyPosition();

                indexWriter.startPartition(key, keyPosition);
                buildIndexForPartition(sstable, dataFile, key, indexWriter);

                processedBytes += keyPosition - previousKeyPosition;
                previousKeyPosition = keyPosition;
            }

            indexWriter.complete();
        }
        catch(IOException e) {
            logger.error("Could not build GraphiteSASI", e);
        }
    }

    protected void buildIndexForPartition(
        SSTableReader sstable, RandomAccessReader dataFile, DecoratedKey key,
        PerSSTableIndexWriter indexWriter
    )
        throws FSReadError
    {
        try {
            RowIndexEntry<?> rowIndexEntry = sstable.getPosition(key, SSTableReader.Operator.EQ);

            // Seek to row position and skip rowKey.
            dataFile.seek(rowIndexEntry.position);
            ByteBufferUtil.skipShortLength(dataFile);

            try (
                SSTableIdentityIterator partition =
                SSTableIdentityIterator.create(sstable, dataFile, key)
            ) {
                while (partition.hasNext()) {
                    indexWriter.nextUnfilteredCluster(partition.next());
                }
            }
        } catch (IOException e) {
            throw new FSReadError(e, sstable.getFilename());
        }
    }
}
