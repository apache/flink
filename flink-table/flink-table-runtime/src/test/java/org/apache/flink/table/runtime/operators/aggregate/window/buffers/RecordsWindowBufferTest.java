/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.operators.aggregate.window.buffers;

import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.operators.window.tvf.combines.RecordsCombiner;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.util.WindowKey;
import org.apache.flink.table.runtime.util.collections.binary.BytesMap;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.EOFException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link RecordsWindowBuffer}. */
class RecordsWindowBufferTest {

    private static final int PAGE_SIZE = (int) MemorySize.parse("32 kb").getBytes();

    /** Minimum memory size must be > {@link BytesMap#INIT_BUCKET_MEMORY_IN_BYTES}(1MB). */
    private static final long MIN_MEMORY_SIZE = MemorySize.parse("2 mb").getBytes();

    private MemoryManager memoryManager;
    private RowDataSerializer keySer;
    private RowDataSerializer valueSer;

    @BeforeEach
    void setUp() {
        memoryManager =
                MemoryManagerBuilder.newBuilder()
                        .setMemorySize(MIN_MEMORY_SIZE)
                        .setPageSize(PAGE_SIZE)
                        .build();

        RowType keyType = new RowType(Arrays.asList(new RowType.RowField("key", new IntType())));
        RowType valueType =
                new RowType(
                        Arrays.asList(
                                new RowType.RowField("key", new IntType()),
                                new RowType.RowField("data", new VarCharType(Integer.MAX_VALUE))));

        keySer = new RowDataSerializer(keyType);
        valueSer = new RowDataSerializer(valueType);
    }

    @AfterEach
    void tearDown() {
        if (memoryManager != null) {
            memoryManager.shutdown();
        }
    }

    private RecordsWindowBuffer createBuffer(RecordsCombiner combiner) {
        return new RecordsWindowBuffer(
                this,
                memoryManager,
                MIN_MEMORY_SIZE,
                combiner,
                keySer,
                valueSer,
                false,
                ZoneId.of("UTC"));
    }

    private static String createLargeString(int sizeInKB) {
        char[] chars = new char[sizeInKB * 1024];
        Arrays.fill(chars, 'x');
        return new String(chars);
    }

    /**
     * Recoverable scenario: buffer has data (numKeys > 0), EOFException triggers flush, retry
     * succeeds.
     */
    @Test
    void testFlushAndRetrySucceeds() throws Exception {
        List<List<RowData>> flushedRecords = new ArrayList<>();
        RecordsCombiner combiner =
                new RecordsCombiner() {
                    @Override
                    public void combine(WindowKey windowKey, Iterator<RowData> records) {
                        List<RowData> batch = new ArrayList<>();
                        while (records.hasNext()) {
                            batch.add(records.next());
                        }
                        flushedRecords.add(batch);
                    }

                    @Override
                    public void close() {}
                };

        try (RecordsWindowBuffer buffer = createBuffer(combiner)) {
            String largeData = createLargeString(100);

            int totalRecords = 50;
            for (int i = 0; i < totalRecords; i++) {
                GenericRowData key = GenericRowData.of(i);
                GenericRowData value = GenericRowData.of(i, StringData.fromString(largeData + i));
                buffer.addElement(key, 1000L, value);
            }

            buffer.flush();

            assertThat(flushedRecords).hasSizeGreaterThanOrEqualTo(2);

            int totalFlushedRecords = flushedRecords.stream().mapToInt(List::size).sum();
            assertThat(totalFlushedRecords).isEqualTo(totalRecords);
        }
    }

    /**
     * Unrecoverable scenario (1st attempt): empty buffer (numKeys == 0), single record too large.
     * Should throw EOFException immediately without flush.
     */
    @Test
    void testFirstUnrecoverableAttemptOnEmptyBuffer() throws Exception {
        final boolean[] flushCalled = {false};
        RecordsCombiner combiner =
                new RecordsCombiner() {
                    @Override
                    public void combine(WindowKey windowKey, Iterator<RowData> records) {
                        flushCalled[0] = true;
                    }

                    @Override
                    public void close() {}
                };

        try (RecordsWindowBuffer buffer = createBuffer(combiner)) {
            String largeString = createLargeString(4 * 1024); // 4MB > 2MB buffer

            GenericRowData key = GenericRowData.of(1);
            GenericRowData value = GenericRowData.of(1, StringData.fromString(largeString));

            assertThatThrownBy(() -> buffer.addElement(key, 1000L, value))
                    .isInstanceOf(EOFException.class);

            assertThat(flushCalled[0]).isFalse();
        }
    }

    /**
     * Tests that minSliceEnd is correctly tracked when an internal flush occurs during addElement()
     * due to buffer overflow.
     */
    @Test
    void testMinSliceEndPreservedAfterInternalFlush() throws Exception {
        List<Long> flushedSliceEnds = new ArrayList<>();
        RecordsCombiner combiner =
                new RecordsCombiner() {
                    @Override
                    public void combine(WindowKey windowKey, Iterator<RowData> records) {
                        flushedSliceEnds.add(windowKey.getWindow());
                        while (records.hasNext()) {
                            records.next();
                        }
                    }

                    @Override
                    public void close() {}
                };

        try (RecordsWindowBuffer buffer = createBuffer(combiner)) {
            String largeData = createLargeString(100);

            // Fill buffer to trigger internal flush on next large record
            int numRecordsToFillBuffer = 18;
            for (int i = 0; i < numRecordsToFillBuffer; i++) {
                GenericRowData key = GenericRowData.of(i);
                GenericRowData value = GenericRowData.of(i, StringData.fromString(largeData + i));
                buffer.addElement(key, 1000L, value);
            }

            flushedSliceEnds.clear();

            // Add record with smaller sliceEnd, triggers internal flush
            GenericRowData key = GenericRowData.of(999);
            GenericRowData value = GenericRowData.of(999, StringData.fromString(largeData + 999));
            buffer.addElement(key, 500L, value);

            flushedSliceEnds.clear();

            // Verify advanceProgress triggers flush for sliceEnd=500
            buffer.advanceProgress(500L);

            assertThat(flushedSliceEnds).contains(500L);
        }
    }

    /**
     * Unrecoverable scenario (after flush): buffer has small records, then oversized record. Flush
     * clears buffer (numKeys = 0), retry still fails. Should throw EOFException.
     */
    @Test
    void testUnrecoverableErrorAfterFlushRetryStillFails() throws Exception {
        List<Integer> flushedKeyIds = new ArrayList<>();
        RecordsCombiner combiner =
                new RecordsCombiner() {
                    @Override
                    public void combine(WindowKey windowKey, Iterator<RowData> records) {
                        while (records.hasNext()) {
                            RowData record = records.next();
                            flushedKeyIds.add(record.getInt(0));
                        }
                    }

                    @Override
                    public void close() {}
                };

        try (RecordsWindowBuffer buffer = createBuffer(combiner)) {
            for (int i = 0; i < 5; i++) {
                GenericRowData key = GenericRowData.of(i);
                GenericRowData value = GenericRowData.of(i, StringData.fromString("small" + i));
                buffer.addElement(key, 1000L, value);
            }

            String largeString = createLargeString(4 * 1024); // 4MB > 2MB buffer
            GenericRowData oversizedKey = GenericRowData.of(999);
            GenericRowData oversizedValue =
                    GenericRowData.of(999, StringData.fromString(largeString));

            assertThatThrownBy(() -> buffer.addElement(oversizedKey, 1000L, oversizedValue))
                    .isInstanceOf(EOFException.class);

            assertThat(flushedKeyIds).containsExactly(0, 1, 2, 3, 4);
        }
    }
}
