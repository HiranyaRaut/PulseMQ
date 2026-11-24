package io.pulsemq.storage;

import io.pulsemq.common.Record;
import io.pulsemq.common.RecordBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * LogCompactor that performs key-based deduplication for compacted topics.
 * Retains only the latest value for each record key, discarding superseded records
 * and pruning expired tombstones.
 */
public class LogCleaner {

    private static final Logger log = LoggerFactory.getLogger(LogCleaner.class);

    /**
     * Cleans an individual partition log if compaction is enabled.
     * Returns the number of records removed by compaction.
     */
    public int clean(PartitionLog partitionLog) throws Exception {
        LogConfig config = partitionLog.config();
        if (config.cleanupPolicy() != LogConfig.CleanupPolicy.COMPACT &&
                config.cleanupPolicy() != LogConfig.CleanupPolicy.COMPACT_DELETE) {
            return 0;
        }

        List<LogSegment> allSegments = partitionLog.segments();
        LogSegment activeSeg = partitionLog.activeSegment();
        if (allSegments.size() <= 1) {
            return 0; // Only active segment exists, cannot compact active segment
        }

        // Candidates: all sealed segments excluding the active segment
        List<LogSegment> candidateSegments = new ArrayList<>();
        for (LogSegment seg : allSegments) {
            if (seg != activeSeg) {
                candidateSegments.add(seg);
            }
        }

        if (candidateSegments.isEmpty()) {
            return 0;
        }

        // Pass 1: Build Key -> Latest Offset Map
        Map<ByteBuffer, Long> latestOffsetMap = new HashMap<>();
        int totalOriginalRecords = 0;

        for (LogSegment seg : candidateSegments) {
            FileRecords records = seg.read(seg.baseOffset(), Integer.MAX_VALUE);
            List<RecordBatch> batches = records.readBatches();
            for (RecordBatch batch : batches) {
                for (Record r : batch.records()) {
                    totalOriginalRecords++;
                    if (r.key() != null) {
                        latestOffsetMap.put(ByteBuffer.wrap(r.key()), r.offset());
                    }
                }
            }
        }

        if (latestOffsetMap.isEmpty()) {
            return 0;
        }

        // Pass 2: Filter and retain only records that are the latest for their key
        long now = System.currentTimeMillis();
        List<Record> retainedRecords = new ArrayList<>();
        List<Long> oldBaseOffsets = new ArrayList<>();

        for (LogSegment seg : candidateSegments) {
            oldBaseOffsets.add(seg.baseOffset());
            FileRecords records = seg.read(seg.baseOffset(), Integer.MAX_VALUE);
            List<RecordBatch> batches = records.readBatches();
            for (RecordBatch batch : batches) {
                for (Record r : batch.records()) {
                    boolean keep = true;
                    if (r.key() != null) {
                        Long latestOffset = latestOffsetMap.get(ByteBuffer.wrap(r.key()));
                        if (latestOffset != null && latestOffset != r.offset()) {
                            keep = false; // Superseded by a newer write
                        } else if (r.value() == null || r.value().length == 0) {
                            // Tombstone pruning if older than deleteRetentionMs
                            if (now - r.timestamp() > config.deleteRetentionMs()) {
                                keep = false;
                            }
                        }
                    }

                    if (keep) {
                        retainedRecords.add(r);
                    }
                }
            }
        }

        int removedCount = totalOriginalRecords - retainedRecords.size();
        if (removedCount == 0) {
            return 0; // No duplicates or tombstones to prune
        }

        log.info("Compacting {}: pruning {} superseded records out of {} total",
                partitionLog.topicPartition(), removedCount, totalOriginalRecords);

        // Build new compacted segment(s)
        long firstBaseOffset = candidateSegments.get(0).baseOffset();
        File partitionDir = partitionLog.dir();
        File tempCompactedDir = new File(partitionDir, "compacted-" + UUID.randomUUID());
        if (!tempCompactedDir.mkdirs()) {
            throw new IOException("Failed to create temporary compaction dir: " + tempCompactedDir.getAbsolutePath());
        }

        LogSegment cleanedSegment = new LogSegment(tempCompactedDir, firstBaseOffset, config);
        try {
            if (!retainedRecords.isEmpty()) {
                // Batch records into reasonable batch chunks (e.g. up to 1000 records or 64KB)
                RecordBatch.Builder builder = null;
                for (Record r : retainedRecords) {
                    if (builder == null) {
                        builder = RecordBatch.builder(r.offset());
                    }
                    builder.append(r);
                    // Flush batch periodically
                    if (builder.build().recordsCount() >= 500) {
                        cleanedSegment.append(builder.build());
                        builder = null;
                    }
                }
                if (builder != null) {
                    cleanedSegment.append(builder.build());
                }
            }

            cleanedSegment.flush();

            // Move files from tempCompactedDir to partitionDir
            String prefix = String.format("%020d", firstBaseOffset);
            File newLog = new File(partitionDir, prefix + ".cleaned.log");
            File newIdx = new File(partitionDir, prefix + ".cleaned.index");
            File newTime = new File(partitionDir, prefix + ".cleaned.timeindex");

            File srcLog = new File(tempCompactedDir, prefix + ".log");
            File srcIdx = new File(tempCompactedDir, prefix + ".index");
            File srcTime = new File(tempCompactedDir, prefix + ".timeindex");

            cleanedSegment.close();

            // Delete existing old candidate segment files
            for (LogSegment seg : candidateSegments) {
                seg.delete();
            }

            srcLog.renameTo(new File(partitionDir, prefix + ".log"));
            srcIdx.renameTo(new File(partitionDir, prefix + ".index"));
            srcTime.renameTo(new File(partitionDir, prefix + ".timeindex"));

            // Reopen replaced segment in partitionDir
            LogSegment replacedSeg = new LogSegment(partitionDir, firstBaseOffset, config);
            partitionLog.replaceSegments(oldBaseOffsets, List.of(replacedSeg));

        } finally {
            tempCompactedDir.delete();
        }

        return removedCount;
    }
}
