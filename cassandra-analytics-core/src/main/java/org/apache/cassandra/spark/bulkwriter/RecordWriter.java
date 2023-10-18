/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.spark.bulkwriter;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.bridge.RowBufferMode;
import org.apache.cassandra.sidecar.common.data.TimeSkewResponse;
import org.apache.cassandra.spark.bulkwriter.token.ReplicaAwareFailureHandler;
import org.apache.cassandra.spark.bulkwriter.token.TokenRangeMapping;
import org.apache.spark.InterruptibleIterator;
import org.apache.spark.TaskContext;
import scala.Tuple2;

import static org.apache.cassandra.spark.utils.ScalaConversionUtils.asScalaIterator;

@SuppressWarnings({ "ConstantConditions" })
public class RecordWriter implements Serializable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RecordWriter.class);
    private static final String WRITE_PHASE = "UploadAndCommit";
    private final BulkWriterContext writerContext;
    private final String[] columnNames;
    private Supplier<TaskContext> taskContextSupplier;
    private final BiFunction<BulkWriterContext, Path, SSTableWriter> tableWriterSupplier;
    private SSTableWriter sstableWriter = null;

    private final BulkWriteValidator writeValidator;
    private final ReplicaAwareFailureHandler<RingInstance> failureHandler;
    private final Set<StreamSession> sessions = new HashSet<>();
    private int batchNumber = 0;
    private int batchSize = 0;

    public RecordWriter(BulkWriterContext writerContext, String[] columnNames)
    {
        this(writerContext, columnNames, TaskContext::get, SSTableWriter::new);
    }

    @VisibleForTesting
    RecordWriter(BulkWriterContext writerContext,
                 String[] columnNames,
                 Supplier<TaskContext> taskContextSupplier,
                 BiFunction<BulkWriterContext, Path, SSTableWriter> tableWriterSupplier)
    {
        this.writerContext = writerContext;
        this.columnNames = columnNames;
        this.taskContextSupplier = taskContextSupplier;
        this.tableWriterSupplier = tableWriterSupplier;
        this.failureHandler = new ReplicaAwareFailureHandler<>(writerContext.cluster().getPartitioner());
        this.writeValidator = new BulkWriteValidator(writerContext, failureHandler);

        writerContext.cluster().startupValidate();
    }

    private Range<BigInteger> getTokenRange(TaskContext taskContext)
    {
        return writerContext.job().getTokenPartitioner().getTokenRange(taskContext.partitionId());
    }

    private String getStreamId(TaskContext taskContext)
    {
        return String.format("%d-%s", taskContext.partitionId(), UUID.randomUUID());
    }

    public List<StreamResult> write(Iterator<Tuple2<DecoratedKey, Object[]>> sourceIterator)
    {
        TaskContext taskContext = taskContextSupplier.get();
        Range<BigInteger> taskTokenRange = getTokenRange(taskContext);
        TokenRangeMapping<RingInstance> tokenRangeMapping = writerContext.cluster().getTokenRangeMapping(false);
        Set<RingInstance> initialTokenRangeInstances = getTokenRangeInstances(tokenRangeMapping, taskTokenRange);

        writeValidator.validateClOrFail(tokenRangeMapping);
        writeValidator.setPhase(WRITE_PHASE);

        // for all replicas in this partition
        validateAcceptableTimeSkewOrThrow(new ArrayList<>(initialTokenRangeInstances));

        LOGGER.info("[{}]: Processing bulk writer partition", taskContext.partitionId());
        scala.collection.Iterator<scala.Tuple2<DecoratedKey, Object[]>> dataIterator =
        new InterruptibleIterator<>(taskContext, asScalaIterator(sourceIterator));
        StreamSession streamSession = createStreamSession(taskContext);
        validateAcceptableTimeSkewOrThrow(streamSession.replicas);
        int partitionId = taskContext.partitionId();
        JobInfo job = writerContext.job();
        Path baseDir = Paths.get(System.getProperty("java.io.tmpdir"),
                                 job.getId().toString(),
                                 Integer.toString(taskContext.stageAttemptNumber()),
                                 Integer.toString(taskContext.attemptNumber()),
                                 Integer.toString(partitionId));
        Map<String, Object> valueMap = new HashMap<>();
        try
        {
            List<RingInstance> exclusions = failureHandler.getFailedInstances();
            Set<Range<BigInteger>> newRanges = tokenRangeMapping.getRangeMap().asMapOfRanges().entrySet()
                                                                .stream()
                                                                .filter(e -> !exclusions.contains(e.getValue()))
                                                                .map(Map.Entry::getKey)
                                                                .collect(Collectors.toSet());
            while (dataIterator.hasNext())
            {
                Tuple2<DecoratedKey, Object[]> rowData = dataIterator.next();
                streamSession = maybeCreateStreamSession(taskContext, streamSession, rowData, newRanges, failureHandler);

                sessions.add(streamSession);
                maybeCreateTableWriter(partitionId, baseDir);
                writeRow(rowData, valueMap, partitionId, streamSession.getTokenRange());
                checkBatchSize(streamSession, partitionId, job);
            }

            // Finalize SSTable for the last StreamSession
            if (sstableWriter != null || (streamSession != null && batchSize != 0))
            {
                finalizeSSTable(streamSession, partitionId, sstableWriter, batchNumber, batchSize);
            }
            LOGGER.info("[{}] Done with all writers and waiting for stream to complete", partitionId);

            final List<StreamResult> results = new ArrayList<>();
            for (StreamSession session : sessions)
            {
                results.add(session.close());
            }

            // When instances for the partition's token range have changed within the scope of the task execution,
            // we fail the task for it to be retried
            if (haveTokenRangeMappingsChanged(initialTokenRangeInstances, taskContext))
            {
                String message = String.format("[%s] Token range mappings have changed since the task started", partitionId);
                LOGGER.error(message);
                throw new RuntimeException(message);
            }

            return results;
        }
        catch (Exception exception)
        {
            LOGGER.error("[{}] Failed to write job={}, taskStageAttemptNumber={}, taskAttemptNumber={}",
                         partitionId,
                         job.getId().toString(),
                         taskContext.stageAttemptNumber(),
                         taskContext.attemptNumber());
            throw new RuntimeException(exception);
        }
    }

    private Set<RingInstance> getTokenRangeInstances(TokenRangeMapping<RingInstance> tokenRange, Range<BigInteger> taskTokenRange)
    {
        return tokenRange.getSubRanges(taskTokenRange)
                         .asMapOfRanges()
                         .values()
                         .stream()
                         .flatMap(Collection::stream)
                         .collect(Collectors.toSet());
    }

    /**
     * Creates a new session if we have the current token range intersecting the ranges from write replica-set.
     * If we do find the need to split a range into sub-ranges, we create the corresponding session for the sub-range
     * if the token from the row data belongs to the range.
     */
    private StreamSession maybeCreateStreamSession(TaskContext taskContext,
                                                   StreamSession streamSession,
                                                   Tuple2<DecoratedKey, Object[]> rowData,
                                                   Set<Range<BigInteger>> newRanges,
                                                   ReplicaAwareFailureHandler<RingInstance> failureHandler) throws IOException
    {
        BigInteger token = rowData._1().getToken();
        Range<BigInteger> tokenRange = getTokenRange(taskContext);

        Preconditions.checkState(tokenRange.contains(token),
                                 String.format("Received Token %s outside of expected range %s", token, tokenRange));

        // token range for this partition is not among the write-replica-set ranges
        if (!newRanges.contains(tokenRange))
        {
            Set<Range<BigInteger>> subRanges = getIntersectingSubRanges(newRanges, tokenRange);
            // We have split ranges - likely resulting from pending nodes
            if (subRanges.size() > 1)
            {
                // Create session using sub-range that contains the token from current row
                Range<BigInteger> matchingRange = subRanges.stream().filter(r -> r.contains(token)).findFirst().get();
                Preconditions.checkState(matchingRange != null,
                                         String.format("Received Token %s outside of expected range %s", token, matchingRange));

                if (streamSession != null && streamSession.getTokenRange() == matchingRange)
                {
                    return streamSession;
                }
                else
                {
                    LOGGER.debug(String.format("[{}] Creating stream session for range: %s", matchingRange), taskContext.partitionId());
                    if (streamSession != null && batchSize != 0)
                    {
                        finalizeSSTable(streamSession, taskContext.partitionId(), sstableWriter, batchNumber, batchSize);
                        sstableWriter = null;
                        batchSize = 0;
                    }
                    return new StreamSession(writerContext, getStreamId(taskContext), matchingRange, failureHandler);
                }
            }
        }

        return (streamSession != null) ? streamSession : createStreamSession(taskContext);
    }

    /**
     * Get ranges from the set that intersect and/or overlap with the provided token range
     */
    private Set<Range<BigInteger>> getIntersectingSubRanges(Set<Range<BigInteger>> ranges, Range<BigInteger> tokenRange)
    {
        return ranges.stream()
                     .filter(r -> r.isConnected(tokenRange) && !r.intersection(tokenRange).isEmpty())
                     .collect(Collectors.toSet());
    }

    private boolean haveTokenRangeMappingsChanged(Set<RingInstance> initialInstances, TaskContext taskContext)
    {
        // Get the uncached, current view of the ring to compare with initial ring
        TokenRangeMapping<RingInstance> endTaskMapping = writerContext.cluster().getTokenRangeMapping(false);
        Set<RingInstance> endInstances = getTokenRangeInstances(endTaskMapping, getTokenRange(taskContext));
        return !initialInstances.equals(endInstances);
    }

    private void validateAcceptableTimeSkewOrThrow(List<RingInstance> replicas)
    {
        if (!replicas.isEmpty())
        {
            TimeSkewResponse timeSkewResponse = writerContext.cluster().getTimeSkew(replicas);
            Instant localNow = Instant.now();
            Instant remoteNow = Instant.ofEpochMilli(timeSkewResponse.currentTime);
            Duration range = Duration.ofMinutes(timeSkewResponse.allowableSkewInMinutes);
            if (localNow.isBefore(remoteNow.minus(range)) || localNow.isAfter(remoteNow.plus(range)))
            {
                final String message = String.format("Time skew between Spark and Cassandra is too large. "
                                                     + "Allowable skew is %d minutes. Spark executor time is %s, Cassandra instance time is %s",
                                                     timeSkewResponse.allowableSkewInMinutes, localNow, remoteNow);
                throw new UnsupportedOperationException(message);
            }
        }
    }

    private void writeRow(Tuple2<DecoratedKey, Object[]> rowData,
                          Map<String, Object> valueMap,
                          int partitionId,
                          Range<BigInteger> range) throws IOException
    {
        DecoratedKey key = rowData._1();
        BigInteger token = key.getToken();
        Preconditions.checkState(range.contains(token),
                                 String.format("Received Token %s outside of expected range %s", token, range));
        try
        {
            sstableWriter.addRow(token, getBindValuesForColumns(valueMap, columnNames, rowData._2()));
        }
        catch (RuntimeException exception)
        {
            String message = String.format("[%s]: Failed to write data to SSTable: SBW DecoratedKey was %s",
                                           partitionId, key);
            LOGGER.error(message, exception);
            throw exception;
        }
    }

    /**
     * Stream to replicas; if batchSize is reached, "finalize" SST to "schedule" streamSession
     */
    private void checkBatchSize(final StreamSession streamSession, final int partitionId, final JobInfo job) throws IOException
    {
        if (job.getRowBufferMode() == RowBufferMode.UNBUFFERED)
        {
            batchSize++;
            if (batchSize >= job.getSstableBatchSize())
            {
                finalizeSSTable(streamSession, partitionId, sstableWriter, batchNumber, batchSize);

                sstableWriter = null;
                batchSize = 0;
            }
        }
    }

    private void maybeCreateTableWriter(int partitionId, Path baseDir) throws IOException
    {
        if (sstableWriter == null)
        {
            Path outDir = Paths.get(baseDir.toString(), Integer.toString(++batchNumber));
            Files.createDirectories(outDir);

            sstableWriter = tableWriterSupplier.apply(writerContext, outDir);
            LOGGER.info("[{}][{}] Created new SSTable writer", partitionId, batchNumber);
        }
    }

    private static Map<String, Object> getBindValuesForColumns(Map<String, Object> map, String[] columnNames, Object[] values)
    {
        assert values.length == columnNames.length : "Number of values does not match the number of columns " + values.length + ", " + columnNames.length;
        for (int i = 0; i < columnNames.length; i++)
        {
            map.put(columnNames[i], values[i]);
        }
        return map;
    }

    private void finalizeSSTable(StreamSession streamSession,
                                 int partitionId,
                                 SSTableWriter sstableWriter,
                                 int batchNumber,
                                 int batchSize) throws IOException
    {
        LOGGER.info("[{}][{}] Closing writer and scheduling SStable stream with {} rows",
                    partitionId, batchNumber, batchSize);
        sstableWriter.close(writerContext, partitionId);
        streamSession.scheduleStream(sstableWriter);
    }

    private StreamSession createStreamSession(TaskContext taskContext)
    {
        Range<BigInteger> tokenRange = getTokenRange(taskContext);
        LOGGER.info("[{}] Creating stream session for range={}", taskContext.partitionId(), tokenRange);
        return new StreamSession(writerContext, getStreamId(taskContext), tokenRange, failureHandler);
    }
}
