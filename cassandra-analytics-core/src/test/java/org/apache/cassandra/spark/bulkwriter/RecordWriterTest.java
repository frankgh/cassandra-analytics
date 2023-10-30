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

import java.math.BigInteger;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.cassandra.bridge.RowBufferMode;
import org.apache.cassandra.spark.bulkwriter.token.TokenRangeMapping;
import org.apache.cassandra.spark.common.model.CassandraInstance;
import org.mockito.Mockito;
import scala.Tuple2;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.when;

public class RecordWriterTest
{
    public static final int REPLICA_COUNT = 3;
    public static final int FILES_PER_SSTABLE = 8;
    public static final int UPLOADED_TABLES = 3;
    private static final String[] COLUMN_NAMES = {
    "id", "date", "course", "marks"
    };

    @TempDir
    public Path folder; // CHECKSTYLE IGNORE: Public mutable field for parameterized testing

    private TokenRangeMapping<RingInstance> tokenRangeMapping;
    private RecordWriter rw;
    private MockTableWriter tw;
    private Tokenizer tokenizer;
    private Range<BigInteger> range;
    private MockBulkWriterContext writerContext;
    private TestTaskContext tc;

    @BeforeEach
    public void setUp()
    {
        tw = new MockTableWriter(folder.getRoot());
        tokenRangeMapping = TokenRangeMappingUtils.buildTokenRangeMapping(100000, ImmutableMap.of("DC1", 3), 12);
        writerContext = new MockBulkWriterContext(tokenRangeMapping);
        tc = new TestTaskContext();
        range = writerContext.job().getTokenPartitioner().getTokenRange(tc.partitionId());
        tokenizer = new Tokenizer(writerContext);
    }

    @Test
    public void testSuccessfulWrite()
    {
        Iterator<Tuple2<DecoratedKey, Object[]>> data = generateData(5, true);
        validateSuccessfulWrite(writerContext, data, COLUMN_NAMES);
    }

    @Test
    public void testSuccessfulWriteCheckUploads()
    {
        rw = new RecordWriter(writerContext, COLUMN_NAMES, () -> tc, SSTableWriter::new);
        Iterator<Tuple2<DecoratedKey, Object[]>> data = generateData(5, true);
        rw.write(data);
        Map<CassandraInstance, List<UploadRequest>> uploads = writerContext.getUploads();
        assertThat(uploads.keySet().size(), is(REPLICA_COUNT));  // Should upload to 3 replicas
        assertThat(uploads.values().stream().mapToInt(List::size).sum(), is(REPLICA_COUNT * FILES_PER_SSTABLE * UPLOADED_TABLES));
        List<UploadRequest> requests = uploads.values().stream().flatMap(List::stream).collect(Collectors.toList());
        for (UploadRequest ur : requests)
        {
            assertNotNull(ur.fileHash);
        }
    }

    @Test
    public void testWriteWithConstantTTL()
    {
        MockBulkWriterContext bulkWriterContext = new MockBulkWriterContext(tokenRangeMapping);
        Iterator<Tuple2<DecoratedKey, Object[]>> data = generateData(5, true, false, false);
        validateSuccessfulWrite(bulkWriterContext, data, COLUMN_NAMES);
    }

    @Test
    public void testWriteWithTTLColumn()
    {
        MockBulkWriterContext bulkWriterContext = new MockBulkWriterContext(tokenRangeMapping);
        Iterator<Tuple2<DecoratedKey, Object[]>> data = generateData(5, true, true, false);
        String[] columnNamesWithTtl =
        {
        "id", "date", "course", "marks", "ttl"
        };
        validateSuccessfulWrite(bulkWriterContext, data, columnNamesWithTtl);
    }

    @Test
    public void testWriteWithConstantTimestamp()
    {
        MockBulkWriterContext bulkWriterContext = new MockBulkWriterContext(tokenRangeMapping);
        Iterator<Tuple2<DecoratedKey, Object[]>> data = generateData(5, true, false, false);
        validateSuccessfulWrite(bulkWriterContext, data, COLUMN_NAMES);
    }

    @Test
    public void testWriteWithTimestampColumn()
    {
        MockBulkWriterContext bulkWriterContext = new MockBulkWriterContext(tokenRangeMapping);
        Iterator<Tuple2<DecoratedKey, Object[]>> data = generateData(5, true, false, true);
        String[] columnNamesWithTimestamp =
        {
        "id", "date", "course", "marks", "timestamp"
        };
        validateSuccessfulWrite(bulkWriterContext, data, columnNamesWithTimestamp);
    }

    @Test
    public void testWriteWithTimestampAndTTLColumn()
    {
        MockBulkWriterContext bulkWriterContext = new MockBulkWriterContext(tokenRangeMapping);
        Iterator<Tuple2<DecoratedKey, Object[]>> data = generateData(5, true, true, true);
        String[] columnNames =
        {
        "id", "date", "course", "marks", "ttl", "timestamp"
        };
        validateSuccessfulWrite(bulkWriterContext, data, columnNames);
    }

    @Test
    public void testWriteWithSubRanges()
    {
        MockBulkWriterContext m = Mockito.spy(writerContext);
        TokenPartitioner mtp = Mockito.mock(TokenPartitioner.class);
        when(m.job().getTokenPartitioner()).thenReturn(mtp);

        // Override partition's token range to span across ranges to force a split into sub-ranges
        Range<BigInteger> overlapRange = Range.closed(BigInteger.valueOf(-9223372036854775808L), BigInteger.valueOf(200000));
        when(mtp.getTokenRange(anyInt())).thenReturn(overlapRange);

        rw = new RecordWriter(m, COLUMN_NAMES, () -> tc, SSTableWriter::new);
        Iterator<Tuple2<DecoratedKey, Object[]>> data = generateData(5, true);
        List<StreamResult> res = rw.write(data);
        assertEquals(1, res.size());
        assertNotEquals(overlapRange, res.get(0).tokenRange);
        final Map<CassandraInstance, List<UploadRequest>> uploads = m.getUploads();
        // Should upload to 3 replicas
        assertEquals(uploads.keySet().size(), REPLICA_COUNT);
        assertThat(uploads.values().stream().mapToInt(List::size).sum(), is(REPLICA_COUNT * FILES_PER_SSTABLE * UPLOADED_TABLES));
        List<UploadRequest> requests = uploads.values().stream().flatMap(List::stream).collect(Collectors.toList());
        for (UploadRequest ur : requests)
        {
            assertNotNull(ur.fileHash);
        }
    }

    @Test
    public void testWriteWithSubRanges2()
    {
        MockBulkWriterContext m = Mockito.spy(writerContext);
        TokenPartitioner mtp = Mockito.mock(TokenPartitioner.class);
        when(m.job().getTokenPartitioner()).thenReturn(mtp);
        // Override partition's token range to span across ranges to force a split into sub-ranges
        Range<BigInteger> overlapRange = Range.closed(BigInteger.valueOf(-9223372036854775808L), BigInteger.valueOf(200000));
        when(mtp.getTokenRange(anyInt())).thenReturn(overlapRange);
        rw = new RecordWriter(m, COLUMN_NAMES, () -> tc, SSTableWriter::new);
        int numRows = 3;
        // There should be 2 SSTables since the data rows across 2 batches (0-indexed)
        int numSSTables = (int) Math.ceil((float) numRows / (writerContext.getSstableBatchSize()));

        // Generate rows with specific token values that belong to the second sub-range
        Iterator<Tuple2<DecoratedKey, Object[]>> data = generateCustomData(numRows, 100001);
        List<StreamResult> res = rw.write(data);
        assertEquals(1, res.size());
        assertNotEquals(overlapRange, res.get(0).tokenRange);
        final Map<CassandraInstance, List<UploadRequest>> uploads = m.getUploads();
        // Should upload to 3 replicas
        assertEquals(REPLICA_COUNT, uploads.keySet().size());
        assertEquals(REPLICA_COUNT * FILES_PER_SSTABLE * numSSTables, uploads.values().stream().mapToInt(List::size).sum());
        List<UploadRequest> requests = uploads.values().stream().flatMap(List::stream).collect(Collectors.toList());
        for (UploadRequest ur : requests)
        {
            assertNotNull(ur.fileHash);
        }
    }

    @Test
    public void testWriteWithTokensAcrossSubRanges()
    {
        MockBulkWriterContext m = Mockito.spy(writerContext);
        TokenPartitioner mtp = Mockito.mock(TokenPartitioner.class);
        when(m.job().getTokenPartitioner()).thenReturn(mtp);
        // Override partition's token range to span across ranges to force a split into sub-ranges
        Range<BigInteger> overlapRange = Range.closed(BigInteger.valueOf(-9223372036854775808L), BigInteger.valueOf(200000));
        when(mtp.getTokenRange(anyInt())).thenReturn(overlapRange);
        rw = new RecordWriter(m, COLUMN_NAMES, () -> tc, SSTableWriter::new);
        int numRows = 3;
        Iterator<Tuple2<DecoratedKey, Object[]>> data = generateCustomData(numRows, 99999);
        List<StreamResult> res = rw.write(data);
        // We expect 2 streams since rows belong to different sub-ranges
        assertEquals(2, res.size());
        assertNotEquals(overlapRange, res.get(0).tokenRange);
        final Map<CassandraInstance, List<UploadRequest>> uploads = m.getUploads();
        // Should upload to 3 replicas
        assertEquals((REPLICA_COUNT + 1), uploads.keySet().size());

        // There are a total of 2 SSTable files - One for each sub-range
        // Although the replica-sets for each file were different they will still be 3 for each subrange
        assertEquals(REPLICA_COUNT * FILES_PER_SSTABLE * 2, uploads.values().stream().mapToInt(List::size).sum());
        List<UploadRequest> requests = uploads.values().stream().flatMap(List::stream).collect(Collectors.toList());
        for (UploadRequest ur : requests)
        {
            assertNotNull(ur.fileHash);
        }
    }

    @Test
    public void testCorruptSSTable()
    {
        rw = new RecordWriter(writerContext, COLUMN_NAMES, () -> tc, (wc, path) -> new SSTableWriter(tw.setOutDir(path), path));
        Iterator<Tuple2<DecoratedKey, Object[]>> data = generateData(5, true);
        // TODO: Add better error handling with human-readable exception messages in SSTableReader::new
        // That way we can assert on the exception thrown here
        RuntimeException ex = assertThrows(RuntimeException.class, () -> rw.write(data));
    }

    @Test
    public void testWriteWithOutOfRangeTokenFails()
    {
        rw = new RecordWriter(writerContext, COLUMN_NAMES, () -> tc, (wc, path) -> new SSTableWriter(tw, folder));
        Iterator<Tuple2<DecoratedKey, Object[]>> data = generateData(5, false);
        RuntimeException ex = assertThrows(RuntimeException.class, () -> rw.write(data));
        assertEquals(ex.getMessage(), "java.lang.IllegalStateException: Received Token "
                                      + "5765203080415074583 outside of expected range [-9223372036854775807‥100000]");
    }

    @Test
    public void testAddRowThrowingFails()
    {
        rw = new RecordWriter(writerContext, COLUMN_NAMES, () -> tc, (wc, path) -> new SSTableWriter(tw, folder));
        tw.setAddRowThrows(true);
        Iterator<Tuple2<DecoratedKey, Object[]>> data = generateData(5, true);
        RuntimeException ex = assertThrows(RuntimeException.class, () -> rw.write(data));
        assertEquals(ex.getMessage(), "java.lang.RuntimeException: Failed to write because addRow throws");
    }

    @Test
    public void testBadTimeSkewFails()
    {
        // Mock context returns a 60-minute allowable time skew, so we use something just outside the limits
        long sixtyOneMinutesInMillis = TimeUnit.MINUTES.toMillis(61);
        rw = new RecordWriter(writerContext, COLUMN_NAMES, () -> tc, (wc, path) -> new SSTableWriter(tw, folder));
        writerContext.setTimeProvider(() -> System.currentTimeMillis() - sixtyOneMinutesInMillis);
        Iterator<Tuple2<DecoratedKey, Object[]>> data = generateData(5, true);
        RuntimeException ex = assertThrows(RuntimeException.class, () -> rw.write(data));
        assertThat(ex.getMessage(), startsWith("Time skew between Spark and Cassandra is too large. Allowable skew is 60 minutes. Spark executor time is "));
    }

    @Test
    public void testTimeSkewWithinLimitsSucceeds()
    {
        // Mock context returns a 60-minute allowable time skew, so we use something just inside the limits
        long fiftyNineMinutesInMillis = TimeUnit.MINUTES.toMillis(59);
        long remoteTime = System.currentTimeMillis() - fiftyNineMinutesInMillis;
        rw = new RecordWriter(writerContext, COLUMN_NAMES, () -> tc, SSTableWriter::new);
        writerContext.setTimeProvider(() -> remoteTime);  // Return a very low "current time" to make sure we fail if skew is too bad
        Iterator<Tuple2<DecoratedKey, Object[]>> data = generateData(5, true);
        rw.write(data);
    }

    @DisplayName("Write 20 rows, in unbuffered mode with BATCH_SIZE of 2")
    @Test()
    void writeUnbuffered()
    {
        int numberOfRows = 20;
        Iterator<Tuple2<DecoratedKey, Object[]>> data = generateData(numberOfRows, true);
        validateSuccessfulWrite(writerContext, data, COLUMN_NAMES, (int) Math.ceil(numberOfRows / writerContext.getSstableBatchSize()));
    }

    @DisplayName("Write 20 rows, in buffered mode with SSTABLE_DATA_SIZE_IN_MB of 10")
    @Test()
    void writeBuffered()
    {
        int numberOfRows = 20;
        Iterator<Tuple2<DecoratedKey, Object[]>> data = generateData(numberOfRows, true);
        writerContext.setRowBufferMode(RowBufferMode.BUFFERED);
        writerContext.setSstableDataSizeInMB(10);
        // only a single data sstable file is created
        validateSuccessfulWrite(writerContext, data, COLUMN_NAMES, 1);
    }

    private void validateSuccessfulWrite(MockBulkWriterContext writerContext,
                                         Iterator<Tuple2<DecoratedKey, Object[]>> data,
                                         String[] columnNames)
    {
        validateSuccessfulWrite(writerContext, data, columnNames, UPLOADED_TABLES);
    }

    private void validateSuccessfulWrite(MockBulkWriterContext writerContext,
                                         Iterator<Tuple2<DecoratedKey, Object[]>> data,
                                         String[] columnNames,
                                         int uploadedTables)
    {
        RecordWriter rw = new RecordWriter(writerContext, columnNames, () -> tc, SSTableWriter::new);
        rw.write(data);
        Map<CassandraInstance, List<UploadRequest>> uploads = writerContext.getUploads();
        assertThat(uploads.keySet().size(), is(REPLICA_COUNT));  // Should upload to 3 replicas
        assertThat(uploads.values().stream().mapToInt(List::size).sum(), is(REPLICA_COUNT * FILES_PER_SSTABLE * uploadedTables));
        List<UploadRequest> requests = uploads.values().stream().flatMap(List::stream).collect(Collectors.toList());
        for (UploadRequest ur : requests)
        {
            assertNotNull(ur.fileHash);
        }
    }

    private Iterator<Tuple2<DecoratedKey, Object[]>> generateData(int numValues, boolean onlyInRange)
    {
        return generateData(numValues, onlyInRange, false, false);
    }

    private Iterator<Tuple2<DecoratedKey, Object[]>> generateData(int numValues, boolean onlyInRange, boolean withTTL, boolean withTimestamp)
    {
        Stream<Tuple2<DecoratedKey, Object[]>> source = IntStream.iterate(0, integer -> integer + 1).mapToObj(index -> {
            Object[] columns;
            if (withTTL && withTimestamp)
            {
                columns = new Object[]
                          {
                          index, index, "foo" + index, index, index * 100, System.currentTimeMillis() * 1000
                          };
            }
            else if (withTimestamp)
            {
                columns = new Object[]
                          {
                          index, index, "foo" + index, index, System.currentTimeMillis() * 1000
                          };
            }
            else if (withTTL)
            {
                columns = new Object[]
                          {
                          index, index, "foo" + index, index, index * 100
                          };
            }
            else
            {
                columns = new Object[]
                          {
                          index, index, "foo" + index, index
                          };
            }
            return Tuple2.apply(tokenizer.getDecoratedKey(columns), columns);
        });
        if (onlyInRange)
        {
            source = source.filter(val -> range.contains(val._1.getToken()));
        }
        Stream<Tuple2<DecoratedKey, Object[]>> limitedStream = source.limit(numValues);
        if (onlyInRange)
        {
            return limitedStream.sorted((o1, o2) -> o1._1.compareTo(o2._1))
                                .iterator();
        }
        return limitedStream.iterator();
    }

    private Iterator<Tuple2<DecoratedKey, Object[]>> generateCustomData(int numValues, int start)
    {
        List<Tuple2<DecoratedKey, Object[]>> res = new ArrayList<>();
        int index = start;
        for (int i = 0; i < numValues; i++)
        {
            final Object[] columns =
            {
            index, index, "foo" + index, index
            };
            DecoratedKey dKey = tokenizer.getDecoratedKey(columns);
            res.add(Tuple2.apply(new DecoratedKey(BigInteger.valueOf(index), dKey.getKey()), columns));
            index++;
        }

        return res.stream().iterator();
    }
}
